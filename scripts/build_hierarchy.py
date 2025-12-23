import os
import duckdb
import polars as pl
import networkx as nx

#  Dynamic root to ensure paths work in docker
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'music_warehouse.duckdb')

def get_market_share_group(parent_name):
    """
    Categorizes a parent label name into one of the major market groups.
    This logic handles case-insensitive matching for the big 3 and their 
    various corporate incarnations and historical parent companies.
    """
    if not parent_name:
        return 'Independent / Unknown'
    
    name_lower = parent_name.lower()
    
    # Universal Music Group (owned by Vivendi, historically MCA, PolyGram)
    universal_patterns = [
        'universal music', 'umg', 'vivendi', 'polygram', 
        'mca records', 'mca inc', 'interscope', 'def jam',
        'geffen', 'island records', 'republic records', 'verve'
    ]
    for pattern in universal_patterns:
        if pattern in name_lower:
            return 'Universal Music Group'
    
    # Sony Music Entertainment (owned by Sony Corp, historically CBS, BMG)
    sony_patterns = [
        'sony music', 'sony corporation', 'ソニー', 'sony group',
        'cbs records', 'cbs corporation', 'bmg', 'columbia records',
        'epic records', 'rca records', 'arista', 'legacy recordings'
    ]
    for pattern in sony_patterns:
        if pattern in name_lower:
            return 'Sony Music Entertainment'
    
    # Warner Music Group (owned by Access Industries, historically Time Warner, AT&T)
    warner_patterns = [
        'warner music', 'warner records', 'warner bros', 
        'at&t', 'time warner', 'atlantic records', 'elektra',
        'rhino', 'parlophone', 'reprise', 'sire records',
        'access industries', 'wea', 'warner communications'
    ]
    for pattern in warner_patterns:
        if pattern in name_lower:
            return 'Warner Music Group'
    
    return 'Independent / Other'

def build_hierarchy():
    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    print("Fetching relationships and labels...")
    
    # Fetch all edges (relationships) where ownership is defined
    # MusicBrainz direction semantics:
    #   'forward' = this label (child_label_id) OWNS the target (parent_label_id)
    #   'backward' = target (parent_label_id) OWNS this label (child_label_id)
    # 
    # We want edges: subsidiary -> owner (so we can traverse UP to find ultimate owner)
    # 
    # For 'forward' direction: child is owner, parent is owned -> edge: parent -> child
    # For 'backward' direction: parent is owner, child is owned -> edge: child -> parent
    edges_query = """
        SELECT 
            CASE 
                WHEN direction = 'forward' THEN parent_label_id  -- owned label
                ELSE child_label_id  -- owned label
            END as from_id,
            CASE 
                WHEN direction = 'forward' THEN child_label_id   -- owner
                ELSE parent_label_id  -- owner
            END as to_id
        FROM int_label_relationships 
        WHERE relationship_type IN ('label ownership', 'imprint')
        AND parent_label_id IS NOT NULL
        AND child_label_id IS NOT NULL
        AND direction IS NOT NULL
    """
    df_edges = con.sql(edges_query).pl()

    # Fetch all label names to map IDs back to readable names later
    labels_query = """
        SELECT label_id, label_name 
        FROM stg_musicbrainz_labels
    """
    df_labels = con.sql(labels_query).pl()
    
    # Create a lookup dictionary for names: ID -> Name
    label_name_map = dict(zip(df_labels["label_id"], df_labels["label_name"]))
    print(f"Building graph with {len(df_edges)} relationships...")
    
    # Initialize a Directed Graph and add edges from polars df
    G = nx.DiGraph()
    G.add_edges_from(df_edges.iter_rows()) # iter_rows to keep memory usage low
    print("Calculating ultimate parents...")
    results = []
    
    # We iterate through every label in our database to determine its owner.
    # If a label is not in the graph (no ownership data), it owns itself.
    for label_id in df_labels["label_id"]:
        ultimate_parent_id = label_id
        path_depth = 0
        
        if label_id in G:
            # Traverse the graph upwards (via edges) to find the ultimate owner.
            # Edges go: subsidiary -> owner, so we use descendants() to follow edges forward.
            # We look for nodes with out_degree=0 (no outgoing edges = no owner above = they are the root).
            try:
                # Get all reachable nodes via outgoing edges (the ownership chain upward)
                reachable = list(nx.descendants(G, label_id))
                
                if reachable:
                    # Find roots: nodes with out_degree == 0 (no owner above them)
                    roots = [n for n in reachable if G.out_degree(n) == 0]
                    if roots:
                        # Pick the first root found (could enhance to pick best match)
                        ultimate_parent_id = roots[0]
                        # Calculate path depth as shortest path length to this root
                        try:
                            path_depth = nx.shortest_path_length(G, label_id, ultimate_parent_id)
                        except:
                            path_depth = len(reachable)
                    else:
                        # Circular ownership - pick the furthest reachable node
                        ultimate_parent_id = reachable[-1] if reachable else label_id
            except Exception:
                # Fallback for graph errors (like cycles)
                pass

        # Resolve names
        label_name = label_name_map.get(label_id, "Unknown")
        parent_name = label_name_map.get(ultimate_parent_id, "Unknown")
        
        # Calculate the market group
        market_group = get_market_share_group(parent_name)

        results.append({
            "label_id": label_id,
            "label_name": label_name,
            "ultimate_parent_name": parent_name,
            "market_share_group": market_group,
            "hierarchy_depth": path_depth
        })

    print("Constructing result DataFrame...")
    df_results = pl.DataFrame(results)

    print("Writing dim_labels table to DuckDB...")
    # Register the Polars dataframe as a view DuckDB can read
    con.register('df_results_view', df_results)
    
    con.execute("CREATE OR REPLACE TABLE dim_labels AS SELECT * FROM df_results_view")
    
    # Cleanup
    con.unregister('df_results_view')
    con.close()
    
    print("Hierarchy build complete.")

if __name__ == "__main__":
    build_hierarchy()