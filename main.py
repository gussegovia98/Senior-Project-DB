import pandas
import sqlite3
import numpy
import pandas as pd


def adjust_for_db(df, rows):
    for row in rows:
        df = df[df[row].notna()]

    df2 = df[rows].copy()

    df2.rename(columns={'asset_owner_user_username': 'username',
                        'asset_owner_address': 'owneradress',
                        'asset_owner_profile_img_url': 'profileimageurl',
                        'asset_image_url': 'imageurl',
                        'asset_asset_contract_created_date': 'nftcreationdate',
                        'asset_collection_name': 'collectionname',
                        'asset_id': 'generalid',
                        'created_date': 'transactiondate',
                        'asset_token_id': 'collectionid',
                        'asset_collection_image_url': 'collectionurl'},
               inplace=True)

    return df2


def push_to_db(df, conn):
    user_row = {'username', 'owneradress', 'profileimageurl', 'imageurl'}
    nft_row = {'imageurl', 'nftcreationdate', 'collectionname', 'generalid',
               'transactiondate', 'collectionid', 'owneradress', 'collectionurl'}

    users_df = df[user_row].copy()
    nft_df = df[nft_row].copy()

    users_df.to_sql('users', con=conn, if_exists='append', index=False)
    nft_df.to_sql('nfts', con=conn, if_exists='append', index=False)


def create_user_table(cur):
    cur.execute('''
    CREATE TABLE users
    (username text, owneradress text, profileimageurl text, imageurl text)
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

    users_table = cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='users';''')
    nft_table = cur.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='nfts';''')
    if users_table.fetchone() == 1 or nft_table.fetchone() == 1:
        create_user_table(cur)
        create_nft_table(cur)

    rows_needed = {'asset_owner_user_username', 'asset_owner_address', 'asset_owner_profile_img_url',
                   'asset_image_url', 'asset_asset_contract_created_date', 'asset_collection_name',
                   'asset_id', 'created_date', 'asset_token_id', 'asset_collection_image_url'}

    df = adjust_for_db(df, rows_needed)
    push_to_db(df, conn)

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
