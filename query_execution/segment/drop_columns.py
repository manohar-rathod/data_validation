class drop_columns:
    def __init__(self, source_df, destination_df):
        self.source_df = source_df
        self.destination_df = destination_df

    def drop_columns_method(self):
        self.source_df.drop(columns=self.columns_name(), axis = 1, inplace = True)
        self.destination_df.drop(columns=self.columns_name(), axis = 1, inplace = True)

        return  self.source_df, self.destination_df


    def columns_name(self):
        return [
            'S_PLAYERCELEBRATION',
            'S_BRANDFOCUS',

            'S_DUETOINCORRECTUMPIRING',

            'S_LIGHTSFAIL',

            'S_BADWEATHER',

            'S_CROWDUNREST',

            'S_SIGHTSCREENADJUSTMENT',

            'S_STREAKERONFIELD',

            'S_BEES',

            'S_DOG',

            'S_BIRD_BIRDS',

            'S_BALLCHANGE',

            'S_DRINKSBREAK',

            'S_ADHOCDRINKSBREAK',

            'S_INNINGSBREAK',

            'S_ROLLINGTHEPITCH',

            'S_BALLLOST',

            'S_CHANGEOFGEAR',

            'S_INTERVIEW',

            'S_EVENTTYPE',
        ]


