from pandas import DataFrame
import pickle
from .rating_calculation import GitlabPrepocessor, GitlabRatingCalculator
from ...rating_utils import GitlabUtils
import logging

MODEL_PATH = '../models/model.pkl'
SCALER_PATH = '../models/scaler.pkl'
def get_preprocessed_gitlab(conn_info: dict) -> DataFrame:
    preproc = GitlabPrepocessor()
    gitlab = GitlabUtils().get_query(conn_info)
    df = preproc.get_feature_dataset(gitlab)
    return df

def get_project_rating(conn_info: dict) -> DataFrame:
    df = get_preprocessed_gitlab(conn_info)
    tpr = GitlabRatingCalculator(df, MODEL_PATH, SCALER_PATH)
    df_project_rating = tpr.calculate_project_rating()
    return df_project_rating

def calculate_gitlab_rating(conn_info: dict) -> DataFrame:
    logging.info("Connecting to Mongo")
    logging.info("Calculating gitlab project rating")
    gitlab_rating_df = get_project_rating(conn_info)
    logging.info("End of script")
    return gitlab_rating_df
