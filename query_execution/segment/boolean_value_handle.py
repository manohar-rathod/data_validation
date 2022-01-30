class boolean_value_handle:
    def __init__(self, source_df, destination_df):
        self.source_df =  source_df
        self.destination_df =  destination_df

    def replace_bool_value(self, w_type, columns):
            for column in columns:
                for index in range(len(self.source_df)):
                    if column in ['S_BOUNDARYCOMMENTATORSPICK',
                      'S_WICKETCOMMENTATORSPICK',
                      'S_UMPIRINGCOMMENTATORSPICK',
                      'S_CAPTAINCYCOMMENTATORSPICK',
                      'S_BOWLERSRUNUPCOMMENTATORSPICK',
                      'S_RUNNINGBWWICKETSCOMMENTATORSPICK',
                      'S_WICKETKEEPINGCOMMENTATORSPICK',
                      'S_BATTINGTYPECOMMENTATORSPICK',
                      'S_BOWLINGTYPECOMMENTATORSPICK',
                      'S_FIELDINGCOMMENTATORSPICK', 'S_COMMENTATORSPICK', 'S_WICKETKEEPINGCOMMENTATORSPICK']:
                        if f"{w_type}:True".upper() in self.source_df[column].values[index].upper():
                            self.source_df[column].values[index] = 'Positive'
                        elif f"{w_type}:False".upper() in self.source_df[column].values[index].upper():
                            self.source_df[column].values[index] = 'Negative'
                        else:
                            self.source_df[column].values[index] = 'None'
                    elif column in ['S_BOUNDARYCOMMENTATORSNAME',
                                'S_WICKETCOMMENTATORSNAME',
                                'S_UMPIRINGCOMMENTATORSNAME',
                                'S_CAPTAINCYCOMMENTATORSNAME',
                                'S_BOWLERSRUNUPCOMMENTATORSNAME',
                                'S_WICKETKEEPINGCOMMENTATORSNAME',
                                'S_BATTINGTYPECOMMENTATORSNAME',
                                'S_BOWLINGTYPECOMMENTATORSNAME',
                                'S_FIELDINGCOMMENTATORSNAME',
                                'S_RUNNINGBWWICKETSCOMMENTATORSNAME','S_COMMENTATORSNAME',
                                'S_WICKETKEEPINGCOMMENTATORSNAME'
                                ]:

                        if f"{w_type}".upper() in self.source_df[column].values[index].upper():
                            self.source_df[column].values[index] = self.source_df[column].values[index].split(":")[0]
                        else:
                            self.source_df[column].values[index] = 'None'

            return self.source_df

    def replace_yes_no(self, column,  lbwappeal_type):
        for index in range(len(self.source_df)):
            if lbwappeal_type.upper() in self.source_df[column].values[index].upper():
                self.source_df[column].values[index] = "Yes"
            else:
                self.source_df[column].values[index] = "No"

        return self.source_df

    def method_replace_yes_no_handle(self):
        self.replace_yes_no('S_LBWAPPEAL', 'Lbw')
        self.replace_yes_no('S_STUMPINGAPPEAL',
                                                                                                  'Stumping')

        self.replace_yes_no(
            'S_HANDLINGTHEBALLAPPEAL',
            'Handlingball')
        self.replace_yes_no(
            'S_OBSTRUCTINGFIELDERAPPEAL',
            'Obstructingfielder')
        self.replace_yes_no(
            'S_RELAY',
            'RELAY')
        self.replace_yes_no(
            'S_DIVE',
            'DIVE')
        self.replace_yes_no(
            'S_JUMP',
            'JUMP')
        self.replace_yes_no(
            'S_POORFIELDING',
            'POORFIELD')

        self.replace_yes_no(
            'S_BACKUP',
            'BACKUP')

        self.replace_yes_no(
            'S_BATSMANWALKINGOFFBEFOREUMPIREDECISION',
            'Batsman Walking Out(Without Umpiring Decision)')

        self.replace_yes_no(
            'S_FIELDERAPPLAUDINGBATSMEN',
            'Fielder Applauding Batsmen')

        self.replace_yes_no(
            'S_FIELDERDECLARINGBOUNDARYONHISOWN',
            "Fielder Declaring A Boundary On His Own")

        self.replace_yes_no(
            'S_ACCEPTINGADROPCATCH',
            "Accepting A Drop Catch")

        self.replace_yes_no(
            'S_UNNECESSARYAPPEALS',
            "Unnecessary Appeals")

        self.replace_yes_no(
            'S_BATSMANNOTACCEPTINGUMPIREDECISION',
            "Batsman Not Accepting Umpire Decision")
        self.replace_yes_no(
            'S_BODYLINEBOWLING',
            "Body Line Bowling")

        self.replace_yes_no(
            'S_NEGATIVEBOWLING',
            "Negative Bowling")

        self.replace_yes_no(
            'S_BALLTAMPERING',
            "Ball Tampering")

        self.replace_yes_no(
            'S_PITCHTAMPERING',
            "Pitch Tampering")

        self.replace_yes_no(
            'S_NORMALCROWD',
            'CROWD')

        self.replace_yes_no(
            'S_CHEERLEADERS',
            'CHEERLEADERS')

        self.replace_yes_no(
            'S_CATCHBYCROWD',
            'CATCHBYCROWD')

        self.replace_yes_no(
            'S_BANNER',
            'BANNER')

        self.replace_yes_no(
            'S_MEXICANWAVES',
            'MEXICANWAVES')

        self.replace_yes_no(
            'S_COMMENTATORSBOX',
            'COMMENTATORSBOX')

        self.replace_yes_no(
            'S_PAVILLIONDUGOUTDRESSINGROOM',
            'PAVILION_DUGOUT')

        self.replace_yes_no(
            'S_SCOREBOARD',
            'SCOREBOARD')

        self.replace_yes_no(
            'S_ELECTRONICDISPLAY',
            'ELECTRONIC_DISPLAY')

        self.replace_yes_no(
            'S_FLAGVIEW',
            'FLAGVIEW')

        self.replace_yes_no(
            'S_GROUND',
            'GROUND')

        self.replace_yes_no(
            'S_OUTSIDESTADIUM',
            'OUTSIDESTADIUM')

        self.replace_yes_no(
            'S_PITCH',
            'PITCH')

        self.replace_yes_no(
            'S_STUMPVIEW',
            'STUMPVIEW')

        self.replace_yes_no(
            'S_TROPHY',
            'TROPHY')

        self.replace_yes_no(
            'S_TEAMWALKINGINTOTHEFIELD',
            'TEAM_WALKING_INTO_THE_FIELD')

        self.replace_yes_no(
            'S_PLAYERWALKINGINTOTHEFIELD',
            'PLAYER_WALKING_INTO_THE_FIELD')

        self.replace_yes_no(
            'S_UMPIRESWALKINGINTOTHEFIELD',
            'UMPIRES_WALKING_INTO_THE_FIELD')

        self.replace_yes_no(
            'S_FIELDERSCLOSINGIN',
            'FIELDERS_CLOSING_IN')

        self.replace_yes_no(
            'S_FIELDINGCAPTAINRECALLINGBATSMAN',
            'Fielding Captain Recalling Batsman')

        self.replace_yes_no(
            'S_DUETOINCORRECTUMPIRING',
            'DUETOINCORRECTUMPIRING')

        self.replace_yes_no(
            'S_BADWEATHER',
            'BADWEATHER')

        self.replace_yes_no(
            'S_CROWDUNREST',
            'CROWDUNREST')

        self.replace_yes_no(
            'S_SIGHTSCREENADJUSTMENT',
            'SIGHTSCREENADJUSTMENT')

        self.replace_yes_no(
            'S_STREAKERONFIELD',
            'STREAKERONFIELD')

        self.replace_yes_no(
            'S_BEES',
            'BEES')

        self.replace_yes_no(
            'S_DOG',
            'DOG')

        self.replace_yes_no(
            'S_BIRD_BIRDS',
            'BIRDBIRDS')

        self.replace_yes_no(
            'S_BALLCHANGE',
            'BALLCHANGE')

        self.replace_yes_no(
            'S_DRINKSBREAK',
            'DRINKSBREAK')

        self.replace_yes_no(
            'S_ADHOCDRINKSBREAK',
            'ADHOCDRINKSBREAK')

        self.replace_yes_no(
            'S_INNINGSBREAK',
            'INNINGSBREAK')

        self.replace_yes_no(
            'S_ROLLINGTHEPITCH',
            'ROLLINGTHEPITCH')

        self.replace_yes_no(
            'S_BALLLOST',
            'BALLLOST')

        self.replace_yes_no(
            'S_CHANGEOFGEAR',
            'CHANGEOFGEAR')

        self.replace_yes_no(
            'S_TOSS',
            'TOSS')

        self.replace_yes_no(
            'S_INTERVIEW',
            'INTERVIEW')

        self.replace_yes_no(
            'S_PRACTICESESSIONS',
            'PRACTICE_SESSION')

        self.replace_yes_no(
            'S_PRESENTATION',
            'PRESENTATION')

        self.replace_yes_no(
            'S_PRIZEDISTRIBUTION',
            'PRIZE_DISTRIBUTION')

        self.replace_yes_no(
            'S_PRESENTATION',
            'PRESENTATION')

        self.replace_yes_no(
            'S_VICTORYLAP',
            'VICTORY_LAP')


        self.replace_bool_value("BOUNDARY", [
            'S_BOUNDARYCOMMENTATORSPICK', "S_BOUNDARYCOMMENTATORSNAME"])
        self.replace_bool_value("WICKET",
            ["S_WICKETCOMMENTATORSPICK",'S_WICKETCOMMENTATORSNAME'])
        self.replace_bool_value("UMPIRING", [
            "S_UMPIRINGCOMMENTATORSPICK", "S_UMPIRINGCOMMENTATORSNAME"])
        self.replace_bool_value("CAPTAINCY", [
            "S_CAPTAINCYCOMMENTATORSPICK", "S_CAPTAINCYCOMMENTATORSNAME"])
        self.replace_bool_value("BOWLERRUNUP", [
            "S_BOWLERSRUNUPCOMMENTATORSPICK", "S_BOWLERSRUNUPCOMMENTATORSNAME"])
        self.replace_bool_value(
            "RUNNINGBETWEENWICKETS", ["S_RUNNINGBWWICKETSCOMMENTATORSPICK", "S_RUNNINGBWWICKETSCOMMENTATORSNAME"])
        self.replace_bool_value("BATTING", [
            "S_BATTINGTYPECOMMENTATORSPICK", "S_BATTINGTYPECOMMENTATORSNAME"])
        self.replace_bool_value("BOWLING", [
            "S_BOWLINGTYPECOMMENTATORSPICK", "S_BOWLINGTYPECOMMENTATORSNAME"])
        self.replace_bool_value("FIELDING", [
            "S_FIELDINGCOMMENTATORSPICK", "S_FIELDINGCOMMENTATORSNAME"])
        self.replace_bool_value("WICKET", [
            "S_WICKETKEEPINGCOMMENTATORSPICK", "S_WICKETKEEPINGCOMMENTATORSNAME"])
        self.replace_bool_value("FIELDING", [
            "S_COMMENTATORSPICK", "S_COMMENTATORSNAME"])

        self.replace_bool_value("WICKETKEEPING", [
            "S_WICKETKEEPINGCOMMENTATORSPICK", "S_WICKETKEEPINGCOMMENTATORSNAME"])

        return self.source_df
