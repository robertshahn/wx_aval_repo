class StationNameConversion:

    MESOWEST_TO_AWS_MAP = {
        'HUR53': 'HUR',
        'MTB42': 'MTB',
        'MAZ22': 'MAZ',
        'WAP55': 'WAS',
        'LAK19': 'LAK',
        'BRN27': 'BRN',
        'STS40': 'STS',
        'STB48': 'stb',
        'LVN11': 'TUM',
        'ALP31': 'ALL',
        'SNO30': 'SSM',
        'BLT41': 'BLT',
        'MSR52': 'MSM',
        'CMT43': 'CMT',
        'CHP55': 'CHL',
        'PVC54': 'PVC',
        'WPS58': 'WPS',
        'MSH33': 'MSH',
        'MHM54': 'MHL',
        'TIM59': 'TML',
        'GVT36': 'SBL'
    }

    AWS_TO_MESOWEST_MAP = {v: k for k, v in MESOWEST_TO_AWS_MAP.items()}

    @staticmethod
    def convert_mesowest_to_aws(station_name):
        return StationNameConversion.MESOWEST_TO_AWS_MAP.get(station_name)

    @staticmethod
    def convert_aws_to_mesowest(station_name):
        return StationNameConversion.AWS_TO_MESOWEST_MAP.get(station_name)
