class ReusableClass:
    def dropColumns(self, df, columns):
        if isinstance(columns, list):
            return df.drop(*columns)
        else:
            return df.drop(columns)
        