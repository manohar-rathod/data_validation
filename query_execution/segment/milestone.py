class milestone_cases:
    def __init__(self, source_df, destination_df):
        self.source_df =  source_df
        self.destination_df =  destination_df


    def milestone_handle(self, column, milstone_type):
        for index in range(len(self.source_df)):
            if column in ['S_BATTINGMILESTONE',
                          "S_BOWLINGMILESTONE",
                          "S_WICKETKEEPINGMILESTONE",
                          "S_FIELDINGMILESTONE",
                          "S_ALLROUNDMILESTONE",
                          'S_PARTNERSHIPMILESTONE',
                          'S_INDIVIDUALMILESTONE',
                          'S_UMPIRINGMILESTONE',
                          ] and milstone_type.upper() in self.source_df['S_MILESTONE'].values[index].upper():
                self.source_df[column].values[index] = self.source_df['S_MILESTONE'].values[index].split(":")[1]

            elif column in ['S_BATTINGMILESTONEDESC',
                            'S_BOWLINGMILESTONEDESC',
                            'S_WICKETKEEPINGMILESTONEDESC'
                             'S_FIELDINGMILESTONEDESC',
                            'S_ALLROUNDMILESTONEDESC',
                            'S_PARTNERSHIPMILESTONEDESC',
                            'S_INDIVIDUALMILESTONEDESC',
                            'S_UMPIRINGMILESTONEDESC',

                            ] and milstone_type.upper() in self.source_df['S_MILESTONE'].values[index].upper():
                self.source_df[column].values[index] = self.source_df['S_MILESTONE'].values[index].split(":")[2]
            else:
                self.source_df[column].values[index] = 'None'


    def milestone(self):
        self.milestone_handle('S_BATTINGMILESTONE', 'BATTINGMILESTONE')
        self.milestone_handle('S_BATTINGMILESTONEDESC', 'BATTINGMILESTONE')
        self.milestone_handle('S_BOWLINGMILESTONE', 'BOWLINGMILESTONE')
        self.milestone_handle('S_BOWLINGMILESTONEDESC', 'BOWLINGMILESTONE')
        self.milestone_handle('S_WICKETKEEPINGMILESTONE', 'WICKETKEEPINGMILESTONE')
        self.milestone_handle('S_WICKETKEEPINGMILESTONEDESC', 'WICKETKEEPINGMILESTONE')
        self.milestone_handle('S_FIELDINGMILESTONE', 'FIELDINGMILESTONE')
        self.milestone_handle('S_FIELDINGMILESTONEDESC', 'FIELDINGMILESTONE')
        self.milestone_handle('S_ALLROUNDMILESTONE', 'ALLROUNDMILESTONE')
        self.milestone_handle('S_ALLROUNDMILESTONEDESC', 'ALLROUNDMILESTONE')
        self.milestone_handle('S_PARTNERSHIPMILESTONE', 'PARTNERSHIPMILESTONE')
        self.milestone_handle('S_PARTNERSHIPMILESTONEDESC', 'PARTNERSHIPMILESTONE')
        self.milestone_handle('S_INDIVIDUALMILESTONEDESC', 'INDIVIDUALMILESTONE')
        self.milestone_handle('S_UMPIRINGMILESTONE', 'UMPIRINGMILESTONE')
        self.milestone_handle('S_UMPIRINGMILESTONEDESC', 'UMPIRINGMILESTONE')
        return self.source_df



