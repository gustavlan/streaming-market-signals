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
    This logic handles case-insensitive matching for the big 3.
    """
    if not parent_name:
        return 'Independent / Unknown'
    
    name_lower = parent_name.lower()
    if 'universal music' in name_lower:
        return 'Universal Music Group'
    if 'sony music' in name_lower:
        return 'Sony Music Entertainment'
    if 'warner music' in name_lower:
        return 'Warner Music Group'
    
    return 'Independent / Other'

def build_hierarchy():
    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    print("Fetching relationships and labels...")
    
    # Fetch all edges (relationships) where ownership is defined
    edges_query = """
        SELECT 
            child_label_id, 
            parent_label_id 
        FROM int_label_relationships 
        WHERE relationship_type IN ('label ownership', 'imprint', 'subsidiary')
        AND parent_label_id IS NOT NULL
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
            # Traverse the graph upwards look for ancestors with an out_degree of 0 (meaning they define no owner).
            # If a cycle exists, this simple logic might fail -> exception.
            try:
                # Get all reachable ancestors
                ancestors = list(nx.ancestors(G, label_id))
                
                if ancestors:
                    # Find the root the ancestor that has no parents (out_degree == 0)
                    # Sort by out_degree to find the 'top' node
                    roots = [n for n in ancestors if G.out_degree(n) == 0]
                    if roots:
                        ultimate_parent_id = roots[0]
                        path_depth = len(ancestors) 
                    else:
                        # If no root is found (circular ownership) stick to the original ID
                        # or pick the furthest ancestor.
                        ultimate_parent_id = ancestors[-1]
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