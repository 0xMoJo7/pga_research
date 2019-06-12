import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
from pathlib import Path
import os
from functools import reduce


def make_numeric(x):
    try:
        return float(re.sub(r'[^\d.]+', '', str(x)))
    except:
        return x


def get_valid_filename(s):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = str(s).strip().replace(' ', '_')
    return re.sub(r'(?u)[^-\w.]', '', s)


def extract_pga_data():

    url = 'https://www.pgatour.com/stats.html'
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')

    soup_links = soup.find_all('a', attrs={'href': re.compile("^/stats/categories")})

    category_links = {}
    for link in soup_links:
        category_links[link.getText()] = link.get("href")

    for category_name, category_link in category_links.items():
        url = f'https://www.pgatour.com{category_link}'
        html = urlopen(url)

        soup = BeautifulSoup(html, 'lxml')
        soup = soup.find('div', class_="section categories")
        soup_links = soup.find_all(attrs={'href': re.compile("^/stats/stat")})

        years = ['2019', '2018', '2017', '2016', '2015']

        for year in years:
            for item in soup_links:
                link = item.get("href")
                position = link.rfind(".")
                subfolder = get_valid_filename(category_name)
                filename = get_valid_filename(item.getText())
                output_dir = Path(f'data/{year}/{subfolder}')

                if os.path.exists(output_dir / f'{filename}.csv'):
                    pass
                else:
                    try:
                        link = f"{link[:position]}.{year}.{link[position+1:]}"

                        df = pd.read_html(f'https://www.pgatour.com{link}')[1]
                        df.columns = [f'{filename}_{column}' for column in df.columns.tolist()]

                        output_dir.mkdir(parents=True, exist_ok=True)

                        df.to_csv(output_dir / f'{filename}.csv')
                    except:
                        pass


def transform_pga_data():

    dataframes_merged = []
    for year in os.listdir('data'):

        dataframes = []
        for category in os.listdir(f'data/{year}'):

            for file in os.listdir(f'data/{year}/{category}'):
                if category not in ['SCORING', 'POINTSRANKINGS', 'MONEYFINISHES'] or file == 'All-Around_Ranking.csv':

                    df = pd.read_csv(f'data/{year}/{category}/{file}').iloc[:, 3:]
                    df = df.rename(columns={df.columns[[0]][0]: 'Player Name'})
                    df = df.drop_duplicates(subset=['Player Name'], keep='first')

                    dataframes.append(df)
                else:
                    pass

        df_merged = reduce(lambda left, right: pd.merge(left, right, on='Player Name', how='outer'), dataframes)
        df_merged['Year'] = int(year)
        dataframes_merged.append(df_merged)

    final_df = reduce(lambda top, bottom: pd.concat([top, bottom], sort=False), dataframes_merged)

    final_df.to_csv('pga_stats.csv', index=False)

extract_pga_data()
transform_pga_data()
