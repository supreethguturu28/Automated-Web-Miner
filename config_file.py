import configparser

config = configparser.ConfigParser()
config.read('config.ini')

mongo_url = config.get('Mongodb Connection', 'url')
database_name = config.get('Mongodb Connection', 'database_name')
collection_name = config.get('Mongodb Connection', 'collection_name')

google_api_key_1 = config.get('Google API', 'api_key_1')
search_engine_id_1 = config.get('Google API', 'search_engine_id_1')
google_api_key_2 = config.get('Google API', 'api_key_2')
search_engine_id_2 = config.get('Google API', 'search_engine_id_2')
google_api_key_3 = config.get('Google API', 'api_key_3')
search_engine_id_3 = config.get('Google API', 'search_engine_id_3')
google_api_key_4 = config.get('Google API', 'api_key_4')
search_engine_id_4 = config.get('Google API', 'search_engine_id_4')

bing_api_key = config.get('Bing API', 'api_key')
bing_custom_instance_id = config.get('Bing API', 'custom_instance_id')

spoonacular_api_key = config.get('Spoonacular API', 'api_key')
