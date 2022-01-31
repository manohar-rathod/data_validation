class cameraview_cases:
    def __init__(self, source_df, destination_df):
        self.source_df = source_df
        self.destination_df = destination_df

    def camera_view_handle(self, column, value_type):
        for index in range(len(self.source_df)):
            if value_type in self.source_df['S_CAMERAVIEW'].values[index].split(","):
                self.source_df[column].values[index] = self.source_df[column].values[index]
            else:
                self.source_df[column].values[index] = 'None'

    def camera_view(self):
        self.camera_view_handle('S_CELEBRITYFOCUS', 'CELEBRITYFOCUS')
        self.camera_view_handle('S_STYLESTATEMENT', 'STYLESTATEMENT')
        self.camera_view_handle('S_BRANDFOCUS', 'BRANDFOCUS')
        self.camera_view_handle('S_USUSALCRICKETGEAR', 'UNUSUALCRICKETGEAR')
        self.camera_view_handle('S_PLAYERINTERVIEW', 'PLAYERINTERVIEW')
        self.camera_view_handle('S_MEMORABILIA', 'MEMORABILIA')
        self.camera_view_handle('S_PLAYERCELEBRATION', 'PLAYER_CELEBRATION')
        return self.source_df
