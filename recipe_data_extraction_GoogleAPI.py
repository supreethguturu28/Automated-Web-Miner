from dbconnect_ import collection
import requests
import time
import random
from bs4 import BeautifulSoup
from bson import ObjectId
import pandas as pd
from urllib.parse import quote
from config_file import (google_api_key_1, google_api_key_2, google_api_key_3, google_api_key_4,
                         search_engine_id_1, search_engine_id_2, search_engine_id_3, search_engine_id_4)


def process_records(api_key, cx, csv_file_path, num_records):
    # Load the CSV file
    df = pd.read_csv(csv_file_path)

    # Define the scenarios and their corresponding field names
    scenarios = [
        {'scenario': 'Recipe', 'field_name': 'content'},
        {'scenario': 'Nutritional Values', 'field_name': 'nutritional_values'},
        {'scenario': 'History', 'field_name': 'history'}
    ]

    # Initialize the list to keep track of Recipe IDs with errors
    error_recipe_ids = []

    # Iterate through records
    for _ in range(num_records):
        try:
            # Find the row with the corresponding Recipe ID where "Content Read" is not 1 or 2
            eligible_rows = df[(df['Recipe ID'] > 0) & ~df['Content Read'].isin([1, 2])]
            if eligible_rows.empty:
                break  # No more eligible records, exit the loop
            row = eligible_rows.iloc[0]

            # Get details from the CSV file
            recipe_id = row['Recipe ID']
            category = row['Category']
            subcategory = row['Sub Category']
            name = row['Name']

            # Skip Recipe IDs that have encountered errors
            if recipe_id in error_recipe_ids:
                continue

            print(f"Processing Recipe ID: {recipe_id}")

            # Initialize the document with default fields
            document = {
                '_id': ObjectId(),  # Automatically generate a unique category_id
                'category': category,
                'subcategory': subcategory,
                'name': name
            }

            skip_record = False  # Flag to skip the record if an error occurs

            errors_occurred_for_scenarios = []  # Track errors for each scenario

            # Process each scenario
            for scenario_info in scenarios:
                scenario = scenario_info['scenario']
                field_name = scenario_info['field_name']

                # Construct the query based on the scenario and user inputs
                query = f"{name}/{scenario}/in english link"
                encoded_query = quote(query)

                # Send a GET request to the Google Custom Search API
                search_url = f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx={cx}&q={encoded_query}"
                response = requests.get(search_url)

                # Check if the request was successful
                if response.status_code == 200:
                    search_results = response.json().get('items', [])

                    # Initialize a string to concatenate content
                    concatenated_content = ""

                    for result in search_results:
                        # Get the link name and URL from the search result
                        link_name = result['title']
                        link_url = result['link']

                        # Fetch the content of the link
                        try:
                            link_response = requests.get(link_url)

                            if link_response.status_code == 200:
                                # Parse the HTML content of the link
                                link_soup = BeautifulSoup(link_response.text, 'html.parser')

                                # Extract and print paragraph content from the link
                                paragraphs = link_soup.find_all('p')
                                content = "\n".join(paragraph.get_text() for paragraph in paragraphs)

                                # Concatenate the content
                                concatenated_content += content
                            else:
                                print(
                                    f"Failed to retrieve content from {link_url}. Status code: {link_response.status_code}")
                                errors_occurred_for_scenarios.append(scenario)
                        except requests.exceptions.RequestException as e:
                            if 'label empty or too long' in str(e):
                                # Handle the specific error and skip the Recipe ID
                                print(f"Skipped Recipe ID {recipe_id} due to the error: {e}")
                                error_recipe_ids.append(recipe_id)
                                skip_record = True
                                break
                            else:
                                print(f"Error fetching content for {link_url}: {e}")
                                errors_occurred_for_scenarios.append(scenario)
                                skip_record = True
                                break  # Skip the record and move to the next one

                    if skip_record:
                        break  # Skip the entire record if there was an error

                    # Set the content in the corresponding field of the document
                    document[field_name] = concatenated_content.encode()  # Store the concatenated content as binary

                else:
                    print(f"Failed to retrieve {scenario} data. Status code: {response.status_code}")
                    errors_occurred_for_scenarios.append(scenario)
                    skip_record = True

            # If any error generated, mark Content Read = 2. If pushed into DB, then mark it as 1
            if any(errors_occurred_for_scenarios):
                df.loc[df['Recipe ID'] == recipe_id, 'Content Read'] = 2

            if not skip_record:
                # Insert the collected content into the MongoDB collection
                collection.insert_one(document)
                df.loc[df['Recipe ID'] == recipe_id, 'Content Read'] = 1
                print(f"Pushed row details for Recipe ID {recipe_id} into MongoDB.")

            # After processing a Recipe ID, add it to the list of error Recipe IDs if errors occurred
            if skip_record or any(errors_occurred_for_scenarios):
                error_recipe_ids.append(recipe_id)

        except Exception as e:
            print(f"An error occurred: {str(e)}")
            error_recipe_ids.append(recipe_id)

        finally:
            # Save the updated CSV file
            df.to_csv(csv_file_path, index=False)

            # Introduce a random wait time between 10 and 15 seconds before processing the next row
            wait_time = random.randint(2, 5)
            print(f"Waiting for {wait_time} seconds...")
            time.sleep(wait_time)


# Example Usage:
api_key = google_api_key_1
cx = search_engine_id_1
csv_file_path = 'C:\\Users\\Supreeth\\PycharmProjects\\SurfTopic\\01_Recipe_Details.csv'
num_of_records = 10
process_records(api_key, cx, csv_file_path, num_of_records)

# from app.config_file import *
# from app.dbconnect_ import collection
# import requests
# import time
# import random
# from bs4 import BeautifulSoup
# from bson import ObjectId
# import pandas as pd
# from urllib.parse import quote
#
# # Define the scenarios and their corresponding field names
# scenarios = [
#     {
#         'scenario': 'Recipe',
#         'field_name': 'content'
#     },
#     {
#         'scenario': 'Nutritional Values',
#         'field_name': 'nutritional_values'
#     },
#     {
#         'scenario': 'History',
#         'field_name': 'history'
#     }
# ]
#
# # API key and custom search engine ID
# api_key = google_api_key_1
# cx = search_engine_id_1
#
# # Load the CSV file
# csv_file_path = "C:\\Users\\Supreeth\\PycharmProjects\\SurfTopic\\01_Recipe_Details.csv"
# df = pd.read_csv(csv_file_path)
#
# # Ask for the number of records to process
# num_records = int(input("Enter the number of records to process: "))
#
# # Initialize the list to keep track of Recipe IDs with errors
# error_recipe_ids = []
#
# # Iterate through rows
# for _ in range(num_records):
#     try:
#         # Find the row with the corresponding Recipe ID where "Content Read" is not 1 or 2
#         eligible_rows = df[(df['Recipe ID'] > 0) & ~df['Content Read'].isin([1, 2])]
#         if eligible_rows.empty:
#             break  # No more eligible records, exit the loop
#         row = eligible_rows.iloc[0]
#
#         # Get the Recipe ID, Category, SubCategory, and Name from the CSV file
#         recipe_id = row['Recipe ID']
#         category = row['Category']
#         subcategory = row['Sub Category']
#         name = row['Name']
#
#         # Skip Recipe IDs that have encountered errors
#         if recipe_id in error_recipe_ids:
#             continue
#
#         print(f"Processing Recipe ID: {recipe_id}")
#
#         # Initialize the document with default fields
#         document = {
#             '_id': ObjectId(),  # Automatically generate a unique category_id
#             'category': category,
#             'subcategory': subcategory,
#             'name': name
#         }
#
#         skip_record = False  # Flag to skip the record if an error occurs
#
#         errors_occurred_for_scenarios = []  # Track errors for each scenario
#
#         for scenario_info in scenarios:
#             scenario = scenario_info['scenario']
#             field_name = scenario_info['field_name']
#
#             # Construct the query based on the scenario and user inputs
#             query = f"{category}/{subcategory}/{name}/{scenario}/in english link"
#             encoded_query = quote(query)
#
#             # Send a GET request to the Google Custom Search API
#             search_url = f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx={cx}&q={encoded_query}"
#             response = requests.get(search_url)
#
#             # Check if the request was successful
#             if response.status_code == 200:
#                 search_results = response.json().get('items', [])
#
#                 # Initialize a string to concatenate content
#                 concatenated_content = ""
#
#                 for result in search_results:
#                     # Get the link name from the search result
#                     link_name = result['title']
#                     # Get the link URL
#                     link_url = result['link']
#
#                     # Fetch the content of the link
#                     try:
#                         link_response = requests.get(link_url)
#
#                         if link_response.status_code == 200:
#                             # Parse the HTML content of the link
#                             link_soup = BeautifulSoup(link_response.text, 'html.parser')
#
#                             # Extract and print paragraph content from the link
#                             paragraphs = link_soup.find_all('p')
#                             content = "\n".join(paragraph.get_text() for paragraph in paragraphs)
#
#                             # Concatenate the content
#                             concatenated_content += content
#                         else:
#                             print(f"Failed to retrieve content from {link_url}. Status code: {link_response.status_code}")
#                             errors_occurred_for_scenarios.append(scenario)
#                     except requests.exceptions.RequestException as e:
#                         if 'label empty or too long' in str(e):
#                             # Handle the specific error and skip the Recipe ID
#                             print(f"Skipped Recipe ID {recipe_id} due to the error: {e}")
#                             error_recipe_ids.append(recipe_id)
#                             skip_record = True
#                             break
#                         else:
#                             print(f"Error fetching content for {link_url}: {e}")
#                             errors_occurred_for_scenarios.append(scenario)
#                             skip_record = True
#                             break  # Skip the record and move to the next one
#
#                 if skip_record:
#                     break  # Skip the entire record if there was an error
#
#                 # Set the content in the corresponding field of the document
#                 document[field_name] = concatenated_content.encode()  # Store the concatenated content as binary
#
#             else:
#                 print(f"Failed to retrieve {scenario} data. Status code: {response.status_code}")
#                 errors_occurred_for_scenarios.append(scenario)
#                 skip_record = True
#
#         # If any error generated mark Content Read = 2. If pushed into DB then 1
#         if any(errors_occurred_for_scenarios):
#             df.loc[df['Recipe ID'] == recipe_id, 'Content Read'] = 2
#
#         if not skip_record:
#             # Insert the collected content into the MongoDB collection
#             collection.insert_one(document)
#             df.loc[df['Recipe ID'] == recipe_id, 'Content Read'] = 1
#             print(f"Pushed row details for Recipe ID {recipe_id} into MongoDB.")
#
#         # After processing a Recipe ID, add it to the list of error Recipe IDs if errors occurred
#         if skip_record or any(errors_occurred_for_scenarios):
#             error_recipe_ids.append(recipe_id)
#
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")
#         error_recipe_ids.append(recipe_id)
#
#     finally:
#         # Save the updated CSV file
#         df.to_csv(csv_file_path, index=False)
#
#         # Introduce a random wait time between 30 and 60 seconds before processing the next row
#         wait_time = random.randint(10, 15)
#         print(f"Waiting for {wait_time} seconds...")
#         time.sleep(wait_time)
