from project_rating.trackers_rating.scripts import calculate_kanban_rating
from project_rating.gitlab_rating.scripts import calculate_gitlab_rating
from configparser import ConfigParser
from datetime import datetime
from footprint_mongoengine import connect, disconnect
from footprint_mongoengine.models import rating


def get_connection() -> dict:
    config = ConfigParser()
    config.read("mongo.ini")

    connection = {"username": config["MongoDB"]["username"],
                  "password": config["MongoDB"]["password"],
                  "ip": config["MongoDB"]["ip"],
                  "port":  config["MongoDB"]["port"],
                  "db": config["MongoDB"]["db"]}

    return connection

def main():
    conn_info = get_connection()
    kanban_rating_df = calculate_kanban_rating(conn_info)
    gitlab_rating_df = calculate_gitlab_rating(conn_info)
    current_dt = datetime.now()
    kanban_rating_df["load_dt"] = current_dt
    #kanban_rating_df.to_json(f"kanban_project_rating_{current_dt.date()}.json", orient='records')
    kanban_rating_df['source'] = kanban_rating_df['source'].apply(rating.SourceEnum)
    connect(**conn_info)
    for row in kanban_rating_df.to_dict(orient="records"):
        rating.ProjectRating(**row).save()
    disconnect()

if __name__ == '__main__':
    main()
