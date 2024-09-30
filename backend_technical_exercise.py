import dask.dataframe as dask_df
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.stats import pearsonr
from typing import List, Tuple
import multiprocessing as mp
import sys
import os

def determine_column_types(dataframe_chunk: pd.DataFrame) -> dict:
    """
    Determine the data type of each column in the given dataframe chunk.
    
    Args:
    dataframe_chunk (pd.DataFrame): A chunk of the dataframe to analyze.
    
    Returns:
    dict: A dictionary mapping column names to their determined types ('string', 'numeric', or 'unknown').
    """
    column_types = {}
    for column_name in dataframe_chunk.columns:
        if pd.api.types.is_string_dtype(dataframe_chunk[column_name]):
            column_types[column_name] = 'string'
        elif pd.api.types.is_numeric_dtype(dataframe_chunk[column_name]):
            column_types[column_name] = 'numeric'
        else:
            column_types[column_name] = 'unknown'
    return column_types

def compare_columns(column1: pd.Series, column2: pd.Series) -> float:
    """
    Compare two columns and return a similarity score.
    
    For string columns, use cosine similarity on hashed vectors.
    For numeric columns, use Pearson correlation coefficient.
    
    Args:
    column1 (pd.Series): First column to compare.
    column2 (pd.Series): Second column to compare.
    
    Returns:
    float: Similarity score between 0 and 1.
    """
    try:
        if pd.api.types.is_string_dtype(column1) and pd.api.types.is_string_dtype(column2):
            # For string columns, use cosine similarity on hashed vectors
            vectorizer = HashingVectorizer(n_features=1000)
            vector1 = vectorizer.fit_transform(column1.astype(str).fillna(''))
            vector2 = vectorizer.transform(column2.astype(str).fillna(''))
            vector1_mean = np.asarray(vector1.mean(axis=0))
            vector2_mean = np.asarray(vector2.mean(axis=0))
            return cosine_similarity(vector1_mean, vector2_mean)[0][0]
        elif pd.api.types.is_numeric_dtype(column1) and pd.api.types.is_numeric_dtype(column2):
            # For numeric columns, use Pearson correlation coefficient
            return abs(pearsonr(column1.fillna(0), column2.fillna(0))[0])
        else:
            # If columns are of different types, return 0 similarity
            return 0.0
    except Exception as e:
        print(f"Error comparing columns: {str(e)}")
        return 0.0

def process_chunk(chunk_df1: pd.DataFrame, full_df2: pd.DataFrame) -> List[Tuple[str, str, float]]:
    """
    Process a chunk of the first dataframe, comparing its columns with all columns of the second dataframe.
    
    Args:
    chunk_df1 (pd.DataFrame): A chunk of the first dataframe.
    full_df2 (pd.DataFrame): The complete second dataframe.
    
    Returns:
    List[Tuple[str, str, float]]: List of tuples containing (column1, column2, similarity_score)
                                  for columns with similarity > 0.8.
    """
    similar_columns = []
    for column_name1 in chunk_df1.columns:
        for column_name2 in full_df2.columns:
            try:
                similarity_score = compare_columns(chunk_df1[column_name1], full_df2[column_name2])
                if similarity_score > 0.8:
                    similar_columns.append((column_name1, column_name2, similarity_score))
            except Exception as e:
                print(f"Error processing columns {column_name1} and {column_name2}: {str(e)}")
    return similar_columns

def main():
    file_path1 = 'SampleData1.csv'
    file_path2 = 'SampleData2.csv'

    # Check if files exist
    if not os.path.exists(file_path1) or not os.path.exists(file_path2):
        print("Error: One or both input files do not exist.")
        sys.exit(1)

    try:
        # Load the second file completely into memory
        print("Loading second file...")
        dataframe2 = dask_df.read_csv(file_path2).compute()

        # Load the first file in chunks to handle large datasets
        print("Loading first file in chunks...")
        dataframe1 = dask_df.read_csv(file_path1, blocksize="10MB")

        # Determine column types for both dataframes
        print("Determining column types...")
        types_df1 = determine_column_types(dataframe1.head(1))
        types_df2 = determine_column_types(dataframe2)

        # Print column types for both dataframes
        print("Column types for Sample 1:")
        for column_name, column_type in types_df1.items():
            print(f"{column_name}: {column_type}")

        print("Column types for Sample 2:")
        for column_name, column_type in types_df2.items():
            print(f"{column_name}: {column_type}")

        print("Comparing columns...")

        # Set up multiprocessing pool
        process_pool = mp.Pool(mp.cpu_count())
        similar_columns_list = []

        # Process each chunk of the first dataframe
        for delayed_chunk in dataframe1.to_delayed():
            try:
                computed_chunk = delayed_chunk.compute()
                chunk_results = process_pool.apply_async(process_chunk, args=(computed_chunk, dataframe2))
                similar_columns_list.extend(chunk_results.get())
            except Exception as e:
                print(f"Error processing chunk: {str(e)}")

        # Close and join the multiprocessing pool
        process_pool.close()
        process_pool.join()

        # Sort the results by similarity score in descending order
        similar_columns_list = sorted(set(similar_columns_list), key=lambda x: x[2], reverse=True)

        # Print the results
        print("Similar columns:")
        for col_name1, col_name2, similarity_score in similar_columns_list:
            print(f"{col_name1} (Sample 1) and {col_name2} (Sample 2) - Similarity: {similarity_score:.2f}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()