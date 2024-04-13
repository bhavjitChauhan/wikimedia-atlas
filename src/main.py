import time
import logging
from pathlib import Path
import networkx as nx

from config import GEFX_FILEPATH
from get import get_page_df, get_pagelinks_df


def main():
    logger = logging.getLogger(__name__)

    page_df = get_page_df()
    pagelinks_df = get_pagelinks_df()

    page_df_size = page_df.shape[0]
    page_df = page_df[page_df['page_namespace'] == 0]
    pagelinks_df = pagelinks_df[pagelinks_df['pl_namespace'] == 0]
    logger.debug(f"Drop nonprimary namespace pages ({
                 page_df_size - page_df.shape[0]} rows removed)")

    page_df_size = page_df.shape[0]
    page_df = page_df.drop_duplicates(subset=['page_title'])
    if page_df_size != page_df.shape[0]:
        logger.warn(
            f"Drop {page_df_size - page_df.shape[0]} duplicate page titles")

    pagelinks_df_size = pagelinks_df.shape[0]
    page_df = page_df[page_df['page_title'] != 'Wikipedia']
    page_df = page_df[~page_df['page_title'].str.startswith('List')]
    pagelinks_df = pagelinks_df[pagelinks_df['pl_title'] != 'Wikipedia']
    pagelinks_df = pagelinks_df[~pagelinks_df['pl_title'].str.startswith(
        'List')]
    if pagelinks_df_size != pagelinks_df.shape[0]:
        logger.debug(f"Drop {pagelinks_df_size -
                     pagelinks_df.shape[0]} meta pagelinks")

    pagelinks_df['page_id'] = pagelinks_df['pl_title'].map(
        page_df.set_index('page_title')['page_id'])
    pagelinks_df_size = pagelinks_df.shape[0]
    pagelinks_df = pagelinks_df.dropna(subset=['page_id'])
    if pagelinks_df_size != pagelinks_df.shape[0]:
        logger.debug(f"Drop {pagelinks_df_size -
                     pagelinks_df.shape[0]} rows with NaN page_id")
    pagelinks_df['page_id'] = pagelinks_df['page_id'].astype('Int64')
    pagelinks_df = pagelinks_df.set_index('page_id')

    augment_start_time = time.perf_counter()
    page_df['pages_linked_from'] = page_df['page_id'].map(
        pagelinks_df['pl_from'].groupby(pagelinks_df.index).apply(list))
    page_df_exploded = page_df.explode('pages_linked_from').groupby(
        'pages_linked_from')['page_id'].apply(list).reset_index()
    page_df_exploded = page_df_exploded.rename(
        columns={'pages_linked_from': 'page_id', 'page_id': 'pages_linked_to'})
    page_df = page_df.merge(page_df_exploded, on='page_id', how='left')
    page_df['num_pages_linked_from'] = page_df['pages_linked_from'].apply(
        lambda x: len(x) if isinstance(x, list) else 0)
    page_df['num_pages_linked_to'] = page_df['pages_linked_to'].apply(
        lambda x: len(x) if isinstance(x, list) else 0)
    logger.debug(f"Augment page_df with pagelinks_df ({
                 (time.perf_counter() - augment_start_time):.2f}s)")

    augment_start_time = time.perf_counter()
    pagelinks_df['pl_from_title'] = pagelinks_df['pl_from'].map(
        page_df.set_index('page_id')['page_title'])
    logger.debug(f"Augment pagelinks_df with page_df ({
                 (time.perf_counter() - augment_start_time):.2f}s)")
    pagelinks_df_size = pagelinks_df.shape[0]
    pagelinks_df.dropna(subset=['pl_from_title'], inplace=True)
    if pagelinks_df_size != pagelinks_df.shape[0]:
        logger.debug(f"Drop {pagelinks_df_size -
                     pagelinks_df.shape[0]} rows with NaN pl_from_title")

    # sort_start_time = time.perf_counter()
    # most_linked_to = page_df.dropna(subset=['pages_linked_to']).set_index('page_id')['pages_linked_to'].apply(len).sort_values(ascending=False).head(10)
    # print(f"Most linked to: {most_linked_to}")
    # most_linked_from = page_df.dropna(subset=['pages_linked_from']).set_index('page_id')['pages_linked_from'].apply(len).sort_values(ascending=False).head(10)
    # print(f"Most linked from: {most_linked_from}")
    # page_df = page_df.sort_values(
    #    by=['num_pages_linked_from', 'num_pages_linked_to'], ascending=False)
    # logger.debug(f"Sort page_df by num_pages_linked_from ({
    #             (time.perf_counter() - sort_start_time):.2f}s)")

    graph_start_time = time.perf_counter()
    G = nx.DiGraph()
    G.add_nodes_from(page_df['page_title'])
    G.add_edges_from(pagelinks_df.reset_index()[
                     ['pl_from_title', 'pl_title']].to_numpy())
    logger.debug(f"Create directed graph G ({
                 (time.perf_counter() - graph_start_time):.2f}s)")

    save_start_time = time.perf_counter()
    Path(GEFX_FILEPATH).parent.mkdir(parents=True, exist_ok=True)
    nx.write_gexf(G, GEFX_FILEPATH)
    logger.debug(f"Save graph G to {GEFX_FILEPATH} ({
                 (time.perf_counter() - save_start_time):.2f}s)")

    print(f"Number of nodes: {G.number_of_nodes()}")
    print(f"Number of edges: {G.number_of_edges()}")

    print(
        f"Average in-degree: {sum(dict(G.in_degree()).values()) / G.number_of_nodes()}")
    print(f"Average degree: {
          sum(dict(G.degree()).values()) / G.number_of_nodes()}")
    # print(f"Average clustering coefficient: {nx.average_clustering(G)}")

    in_degree_start_time = time.perf_counter()
    in_degree = dict(G.in_degree())
    in_degree = {k: v for k, v in sorted(
        in_degree.items(), key=lambda item: item[1], reverse=True)}
    print("Top 10 nodes with the highest in-degree:")
    for i, (k, v) in enumerate(in_degree.items()):
        print(f"\t{k}: {v}")
        if i == 9:
            break
    logger.debug(
        f"Find in-degree of G ({(time.perf_counter() - in_degree_start_time):.2f}s)")

    out_degree_start_time = time.perf_counter()
    out_degree = dict(G.out_degree())
    out_degree = {k: v for k, v in sorted(
        out_degree.items(), key=lambda item: item[1], reverse=True)}
    print("Top 10 nodes with the highest out-degree:")
    for i, (k, v) in enumerate(out_degree.items()):
        print(f"\t{k}: {v}")
        if i == 9:
            break
    logger.debug(
        f"Find out-degree of G ({(time.perf_counter() - out_degree_start_time):.2f}s)")

    connected_components_start_time = time.perf_counter()
    connected_components = list(nx.weakly_connected_components(G))
    print(f"Number of connected components: {len(connected_components)}")
    logger.debug(f"Find connected components of G ({
                 (time.perf_counter() - connected_components_start_time):.2f}s)")

    largest_connected_component_start_time = time.perf_counter()
    largest_connected_component = max(
        connected_components, key=len)
    print(f"Number of nodes in the largest connected component: {
          len(largest_connected_component)}")
    logger.debug(f"Find largest connected component of G ({
                 (time.perf_counter() - largest_connected_component_start_time):.2f}s)")


if __name__ == "__main__":
    main()
