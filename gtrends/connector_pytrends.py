import time

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as logger
from fivetran_connector_sdk import Operations as op
import pandas as pd
import random

from pytrends.request import TrendReq

key_word_list=['allergy', 'allergies', 'zyrtec', 'allergies medicine', 'allergy symptoms', 'allergy medicine',
               'allergy relief', 'symptoms of allergies', 'allergies symptoms', 'allergy medication',
               'nasal congestion', 'medicine for allergies', 'allergy versus cold', 'best allergy pills',
               'allergies vs cold', 'best allergy medicine', 'allergy medications', 'cold or allergies',
               'seasonal allergy symptoms', 'seasonal allergies', 'signs of allergies', 'allergies or cold',
               'top allergy medicines', 'allergy cough', 'over the counter allergy medicine', 'treat allergy',
               'stuffy nose', 'cold vs allergies', 'home remedies for allergies', 'how to get rid of allergies',
               'head cold or allergies', 'itchy throat causes', 'itchy eye allergies', 'allergic rhinitis',
               'what cause watery eyes', 'best over the counter allergy medicine',
               'runny nose cause', 'symptoms for allergy','allergies symptom', 'signs for allergies',
               'allergy versus cold symptoms','cause of itchy eyes', 'claritin or zyrtec', 'zyrtec versus claritin',
               'how to tell if allergy or cold','best allergy medicine for pollen', 'what does zyrtec do',
               'claritin versus zyrtec', 'eye allergies', 'zyrtec vs allegra', 'cold or allergy', 'allery medications',
               'symptoms of allergy', 'signs of allergy', 'how to stop allergies', 'throat allergy', 'allegra vs zyrtec',
               'can allergies turn into a cold', 'seasonal allergy', 'itchy throat allergy symptoms', 'allergy symptom',
               'spring allergies', 'why do I have an itchy throat', 'allegra or zyrtec', 'non drowsy zyrtec',
               'information about allergy', 'claritin vs zyrtec', 'allergie drugs', 'tree pollen allergy cures',
               'zyrtec-d', 'nasal allergies', 'best otc allergy tablets', 'zyrtec allegra', 'winter allergies',
               'information on allergy', 'zyrtec or allegra', 'zyrtec allergy', 'zyrtec d', 'zyrtec allergy pills',
               'allergy relief tablets', 'living with seasonal allergies', 'what is zyrtec',
               'is claritin better than zyrtec', 'relief for allergy congestion', 'how to treat outdoor allergies',
               'what causes itchy throat', 'zyrtec side effects', 'zyrtec tablets', 'animal allergy',
               'ragweed allergy symptoms', 'best otc allergy tabs', "what's causing my allergies today",
               'symptoms and allergy', 'loratadine or cetirizine', 'zyrtec ingredients', 'best allergy tabs',
               'ragweed allergy', 'best treatment for outdoor allergies', 'best otc seasonal allergy treatment',
               'about zyrtec', 'allergy faq', 'zyrtec vs claritin', 'how to relieve outdoor allergies', 'claritin zyrtec',
               'fexofenadine versus cetirizine', 'allergy treatment and decongestion', 'treat indoor allergy',
               'seasonal allergy sufferer', 'liquid gels allergy', 'best otc allergy gels', 'tree pollen seasonal allergy',
               'sick or allergy', 'allegra zyrtec', 'about indoor allergies', 'indoor allergies symptom',
               'about indoor allergy', 'allergy liquid gel pills', 'zyrtec generic', 'dog allergies',
               '10 mg allergy tabs', 'grass pollen medicine', 'outdoor allergy triggers', 'outdoor allergy ragweed',
               'outdoor allergies drugs', 'live with allergy', 'liquid tabs for allergy', 'is zyrtec 12 or 24 hours',
               'indoor allergies faq', 'how to prevent grass allergies', 'allergy relief dissolve tablets',
               'allergy medication liquid gels', 'allergy medication gels', 'allergy gel tablets',
               'allergy and gardening', 'allergies and gardening', 'allergies and garden', '24 hour gels',
               '24 hour allergy tabs']

"""state_code_list=['NY-532', 'GA-524', 'TX-635', 'MD-512', 'MA-506', 'NY-514', 'NC-517', 'OH-515',
                 'OH-510', 'IL-602', 'TX-623', 'OH-542', 'OH-534', 'CO-751', 'MI-505', 'MI-563',
                 'NC-518', 'SC-567', 'CT-533', 'TX-618', 'PA-566', 'IN-527', 'FL-561', 'MO-616',
                 'TN-557', 'NV-839', 'CA-803', 'KY-529', 'TN-640', 'WI-617', 'MN-613', 'FL-686',
                 'TN-659', 'LA-622', 'NY-501', 'VA-544', 'PA-504', 'AZ-753', 'PA-508', 'OR-820',
                 'MA-521', 'NC-560', 'CA-862', 'ID-770', 'TX-641', 'CA-825', 'CA-807', 'MO-609',
                 'WA-819', 'DC-511', 'AL-630', 'FL-528', 'FL-539']"""
state_code_list=['NY-532']

list_list_kewords=[]
random.shuffle(key_word_list)
if len(key_word_list) > 0:
    for i in range(len(key_word_list)):
        if len(key_word_list) > 0:
            list_list_kewords.append(key_word_list[: 5])
            del (key_word_list[: 5])
        else:
            break

def update(configuration: dict, state: dict):
    profile_cursor = state["profile_cursor"] if "profile_cursor" in state else 0

    for s in set(state_code_list):
        final_df = get_data(s)
        yield from upsert_dataframe_approach_3(keyword_df=final_df, state=state, cursor=profile_cursor)
        pass

def get_data(state_code):
    state_wise_dfs=[]

    headers = {
        'accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    }

    total_count=0

    logger.info(f"Running for the State code {state_code}")
    keyword_wise_df = pd.DataFrame()
    for keywords in list_list_kewords:
        logger.fine(state_code)
        logger.fine(keywords)
        #pytrend = TrendReq(retries=20, backoff_factor=10,requests_args={'verify':False})
        pytrend = TrendReq(retries=20, backoff_factor=10, requests_args={'headers':headers})
        logger.fine(pytrend.headers.get("Retry-After"))
        pytrend.build_payload(kw_list=keywords, timeframe='today 5-y', geo=f"US-{state_code}")

        df_temp = pytrend.interest_over_time()
        #logger.fine(str(df_temp.head()))
        # logger.info(f"Total no of record fetched {len(df_temp)}")
        if len(df_temp) > 0:
            df_temp = df_temp.drop(columns='isPartial')
            logger.fine(len(df_temp))
            df_temp.reset_index(inplace=True)
            df_melted = pd.melt(df_temp, id_vars=['date'], value_vars=keywords).rename(
                columns={'variable': 'keyword', 'value': 'relative_score'})
            df_melted['region'] = state_code
            keyword_wise_df = pd.concat([keyword_wise_df, df_melted]).reset_index(drop=True)
            total_count=total_count+len(df_temp)
        else:
            logger.info(f"data is not available for the Stete code :{state_code} with Keyword list {keywords}  ")

    state_wise_dfs.append(keyword_wise_df)
    logger.info(f"Total no of records fetched for the state code {state_code} id {total_count}")
    state_code_list.remove(state_code)

    df_merged = pd.concat(state_wise_dfs)
    df_merged[['state', 'city_code']] = df_merged['region'].str.split('-', expand=True)
    df_merged.index = range(len(df_merged))

    return df_merged


        # data_frame.rename(columns={'GeoAbbr': 'city_code'}, inplace=True)
        # df_merged = df_merged.merge(data_frame[['city_code', 'city_combs']], on='city_code', how='left')
        # df_merged.drop(columns=['state', 'region'], axis=1, inplace=True)
        # df_merged = df_merged[['city_combs', 'city_code', 'date', 'keyword', 'relative_score']]
        # df_merged.head()

def schema(configuration: dict):
    return [
        {
            "table": "t_gtrends_keyword_us",
            # Columns and data types will be inferred by Fivetran
        }
    ]


def upsert_dataframe_approach_3(keyword_df, state, cursor):
    # APPROACH 3: Faster approach, Keeps track of the original indices of the rows, which can be useful for certain operations that require indexing information.
    # UPSERT all login table data.
    # Iterate over the values of the dictionary (which are the DataFrame rows)
    for value in keyword_df.to_dict("index").values():
        logger.fine(value)
        yield op.upsert("t_gtrends_keyword_us", value)

    # Checkpointing at the end of the "login" table data processing
    state["login_cursor"] = cursor

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)

connector = Connector(update=update, schema=schema)

if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary.
    #with open("configuration.json", 'r') as f:
    #    configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()