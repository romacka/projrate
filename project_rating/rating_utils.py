from mongoengine.queryset.visitor import Q
from footprint_mongoengine.models import wekan
from footprint_mongoengine import connect, disconnect
from pymongo import MongoClient


class TrackersUtils:
    
    taiga = "TAIGA"
    wekan = "WEKAN"

    fields = {
                wekan: {
                    "user_id": "assignee",
                    "project_id": "board_id",
                    "hours": "hours",
                    "start_time": "start_at",
                    "end_time": "end_at",
                    "status": "completed"
                },
                taiga: {
                    "user_id": "username",
                    "project_id": "card_project",
                    "hours": "hours",
                    "start_time": "created_date",
                    "end_time": "finished_date",
                    "status": "is_closed"
                }
            }
    
    weights = {
                "w1": 0.21,
                "w2": 0.29,
                "w3": 0.25,
                "w4": 0.25,
                "w11": 0.28,
                "w22": 0.39,
                "w44": 0.33
               }
    
    def get_query(self, source: str, conn_info: dict):
        connect(**conn_info)
        if source==self.taiga:
            taiga_query = {
                    "filtr": {
                        "$or": [
                            {
                                "issues": {
                                    "$not": {
                                        "$size": 0
                                    }
                                }
                            },
                            {
                                "tasks": {
                                    "$not": {
                                        "$size": 0
                                    }
                                }
                            }
                        ]
                    },
                    "select": {
                        "_id": False,
                        "name": 1,
                        "members.full_name": 1,
                        "members.username": 1,
                        "members.id": 1,
                        "tasks.owner": 1,
                        "tasks.assigned_to": 1,
                        "tasks.created_date": 1,
                        "tasks.finished_date": 1,
                        "tasks.is_closed": 1,
                        "tasks.subject": 1,
                        "tasks.task_custom_values.Трудозатраты": 1,
                        "issues.owner": 1,
                        "issues.assigned_to": 1,
                        "issues.created_date": 1,
                        "issues.finished_date": 1,
                        "issues.is_closed": 1,
                        "issues.subject": 1,
                        "issues.issue_custom_values.Трудозатраты": 1
                    }
                }
            #taiga_query = list(wekan.Card.objects(  
            #                        Q(users__ne=None) | Q(info__ne=None)
            #                    ).only("users", "board_id", "card_id", "info__hours",
            #                        "info__hours", "info__start_at", "info__end_at", 
            #                        "info__due_at", "status", "completed").exclude("id").as_pymongo())
            query = taiga_query
        elif source == self.wekan:
            wekan_query = list(wekan.Card.objects(  
                                    Q(users__ne=None) | Q(info__ne=None)
                                ).only("users", "board_id", "card_id", "info__hours",
                                    "info__hours", "info__start_at", "info__end_at", 
                                    "info__due_at", "status", "completed").exclude("id").as_pymongo())
            query = wekan_query
        disconnect()
        return query


class GitlabUtils:

    @staticmethod
    def get_query(conn_info: dict):
        client = MongoClient(conn_info["ip"], username=conn_info["username"], password=conn_info["password"])
        db = client[conn_info["db"]]
        collection = db.git
        return list(collection.find({}, {}))
