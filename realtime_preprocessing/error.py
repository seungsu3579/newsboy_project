class NoRecentNewsDataError(Exception):
    def __init__(self):
        super().__init__('There is no new data in crawling database.')