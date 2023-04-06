import pickle
import pandas as pd
pd.options.mode.chained_assignment = None


class GitlabRateCalculator:
    
    def __init__(self, df, model_path: str, scaler_path: str):
        self.username = df['username']
        self.scaler = self._load_scaler(scaler_path)
        self.df = self.scaler.transform(df.drop(['username', 'namespaceName'], axis=1))
        self.model = self._load_model(model_path)
    def _load_model(self, model_path: str) -> pickle:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
        
    def _load_scaler(self, scaler_path: str) -> pickle:
        with open(scaler_path, 'rb') as f:
            scaler = pickle.load(f)
        return scaler

    def calculate_project_rating(self) -> DataFrame:

        df_project_rate = pd.DataFrame(np.clip(self.model.predict(self.df), 0, 10), columns=['rating'])
        df_project_rate.dropna(inplace=True)
        df_project_rate.reset_index(drop=True, inplace=True)
        df_project_rate = pd.concat([self.username, df_project_rate], axis=1).groupby('username', as_index=False).mean()
        df_project_rate['percentile_rating'] = ((df_project_rate['rating'].rank(method='max', pct=True)*100)-100).abs()
        df_project_rate['percentile_rank'] = (df_project_rate['rating'].rank(method='max')-len(df_project_rate)).abs()+1
        df_project_rate['source']  = 'gitlab'

        return df_project_rate

class GitlabPrepocessor():
    
    def __init__(self):
        pass
        
    def get_feature_dataset(self, gitlab: list) -> DataFrame:
        df = pd.DataFrame(gitlab)
        df = pd.concat([df[['created_at', 'user_id', 'project']], pd.json_normalize(df['stats'])], axis=1)
        df['created_at'] = df['created_at'].apply(lambda x: str(x.year) + str(x.day))
        agg_func_math = {
          'additions': 'sum',
          'deletions': 'sum',
          'created_at': 'nunique'
        }
        df_features = df.groupby(['user_id', 'project']).agg(agg_func_math).reset_index()
        df_features = df_features[['user_id', 'project', 'created_at', 'additions', 'deletions']].rename(columns={"user_id": "username",
                                                                                                    "project": "namespaceName",
                                                                                                    "created_at":"activeMonthCount",
                                                                                                    "additions":"additionsSum",
                                                                                                    "deletions":"deletionsSum"})
        return df_features
