class reusable:
    def dropColumns(self, df, columns):
        df = df.drop(*columns)
        return df

    def dropDuplicates(self, df, subset):
        df = df.dropDuplicates(subset)
        return df