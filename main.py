import pandas
import sqlite3
import numpy


def adjust_for_db(df, rows):
    for row in rows:
        df = df[df[row].notna()]


    return df

def create_user_table(cur):
    cur.execute('''
    CREATE TABLE users
    (username text, adress text, profileimage text, nftimage text)
    ''')


def create_nft_table(cur):
    cur.execute('''
    CREATE TABLE nfts
    (imageurl text, nftcreationdate text, collectionname text, generalid text, transactiondate text, collectionid text,
    owneradress text, collectionurl text)
    ''')


def add_to_db(file):
    # Use a breakpoint in the code line below to debug your script.

    conn = sqlite3.connect('nft.db')
    cur = conn.cursor()
    df = pandas.read_csv(file)

    users_Table = cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='users';''')
    nft_table = cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='nfts';''')
    if users_Table.fetchone() == 1 or nft_table.fetchone() == 1:
        create_user_table(cur)
        create_nft_table(cur)

    rows_needed = {'asset_owner_user_username', 'asset_owner_address', 'asset_owner_profile_img_url',
                   'asset_image_url', 'asset_asset_contract_created_date', 'asset_collection_name',
                   'asset_id', 'created_date', 'asset_token_id', 'asset_collection_image_url'}

    df = adjust_for_db(df, rows_needed)
    print("test")

    # if df['asset_owner_user_username'] == nan :

    # what are our tables gonna look like?
    # I'm thinking user table
    # what are the keys? username def the main key
    # asset_owner_user_username, asset_owner_address
    # asset_owner_profile_img_url,  asset_image_url,
    # they don't have one? we need to use
    # get rid of nans
    #
    #
    #
    #
    # and nft table
    # asset_image_url, asset_asset_contract_created_date // this one is nft creation date,
    # asset_collection_name, asset_id, created_date// this is transcation
    # asset_token_id, asset_owner_address, asset_collection_image_url,
    #
    #
    #


if __name__ == '__main__':
    add_to_db("bored_ape.csv")
