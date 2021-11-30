from __future__ import annotations
import requests
from bs4 import BeautifulSoup
from datetime import date,timedelta
from tqdm import tqdm

import mysql.connector

def sql_connection():
    mydb = mysql.connector.connect(host="5.255.98.125 ",
                                   user = "keshava",
                                   password = 'Dhanam_7',
                                   database = 'football_db'
                                   )

    mycursor = mydb.cursor(prepared=True)
    return mydb, mycursor

def match_table_data(match_day = None, auto_match_day = None) -> list[(tuple)] :
    if match_day is None:
        url = f"https://www.transfermarkt.com/premier-league/spieltagtabelle/wettbewerb/GB1/saison_id/2021"
        match_day = auto_match_day
    else:
        url = f"https://www.transfermarkt.com/premier-league/spieltagtabelle/wettbewerb/GB1?saison_id=2021&spieltag={match_day}"

    html_content = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}).content

    soup = BeautifulSoup(html_content, "html.parser")
    table_div = soup.find_all("div", attrs={"class": "box"})

    for i in table_div:
        header_text = i.find("h2", attrs={"class": "content-box-headline"})
        if header_text:
            if header_text.text == 'Table Premier League 21/22 ':
                div_table = i

    div_table_headers = div_table.find("thead", attrs={"class": ""})
    div_table_headers_text = div_table_headers.find_all("th")

    div_table_row_body = div_table.find("tbody")
    div_table_rows = div_table_row_body.find_all("tr")
    team_data_list = []
    current_date = date.today().strftime("%Y-%m-%d")
    for ind, row in enumerate(div_table_rows):
        team_dict = {"rank": str(ind + 1),
                     "clubName": "",
                     "matches": "",
                     "win": "",
                     "draw": "",
                     "lose": "",
                     "goals": "",
                     "goalDifference": "",
                     "points": "",
                     "date": current_date,
                     "matchDay":match_day}
        row_data_all = row.find_all(lambda tag: tag.name == 'td' and tag.get('class') == ['zentriert'])
        row_data_team = row.find("td", attrs={'class': 'no-border-links hauptlink'})
        team_dict["clubName"] = row_data_team.text.strip()
        column_list = ["matches", "win", "draw", "lose", "goals", "goalDifference", "points", "date", "matchDay"]
        for ind, row_data in enumerate(row_data_all):
            team_dict[column_list[ind]] = row_data.text.strip()

        team_data_list.append(team_dict)

    result = [(d['rank'], d['clubName'], d['matches'], d['win'], d['draw'], d['lose'], d['goals'], d['goalDifference'],
               d['points'], d['date'], d['matchDay']) for d in team_data_list]

    return result

def insert_result_in_table(result):
    mydb, mycursor = sql_connection()
    sql_query= "INSERT INTO premier_league_table (`rank`, clubName, matches, win, draw, lose, goals, goalDifference, points, `date`, matchDay) VALUES (%s, %s, %s, %s, %s ,%s, %s,%s, %s,%s, %s)"

    mycursor.executemany(sql_query, result)
    mydb.commit()

def check_max_match_day_in_table():
    mydb, mycursor = sql_connection()
    sql_query= "SELECT max(`matchDay`) from premier_league_table"
    mycursor.execute(sql_query)
    result = mycursor.fetchone()
    if result:
        return int(result[0])
    else:
        return None

def check_recent_matches():
    mydb, mycursor = sql_connection()
    sql_query= "SELECT matches from premier_league_table order by `date` desc, `rank` asc limit 20"
    mycursor.execute(sql_query)
    result = mycursor.fetchall()
    recent_match_list = [match[0] for match in result]
    return recent_match_list


def main():
    # for i in tqdm(range(12,13)):
    #     result = match_table_data(match_day=i,auto_match_day= None)
    #     insert_result_in_table
    auto_match_day = (check_max_match_day_in_table()) + 1
    if auto_match_day <= 38:
        result = match_table_data(match_day=None, auto_match_day=auto_match_day)
        result_list = [int(table[2]) for table in result]
        recent_match_list = check_recent_matches()
        if result_list == recent_match_list:
            print("Passed")
            pass
        else:
            insert_result_in_table(result)

if __name__ == "__main__":
    main()