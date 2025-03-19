from greenbutton import parse
import pandas as pd

class GreenData:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.start_date = self.get_start_date().strftime('%Y-%m-%d')
        self.end_date = self.get_end_date().strftime('%Y-%m-%d')
    
    def get_start_date(self):
        return self.dataframe['group_key'].iloc[0]
    
    def get_end_date(self):
        return self.dataframe['group_key'].iloc[-1]
    
    def get_subset(self, start_date, end_date):
        start_date = pd.Period(start_date, freq='D')
        end_date = pd.Period(end_date, freq='D')

        # check if start_date and end_date are valid
        if start_date > end_date:
            raise ValueError("start_date must be before end_date")
        if start_date < self.get_start_date():
            raise ValueError("start_date must be after the start date of the dataframe")
        if end_date > self.get_end_date():
            raise ValueError("end_date must be before the end date of the dataframe")
        
        #return self.dataframe[(self.dataframe['group_key'] >= start_date) & (self.dataframe['group_key'] <= end_date)]
        return GreenData(self.dataframe[(self.dataframe['group_key'] >= start_date) & (self.dataframe['group_key'] <= end_date)])
    
    def to_json(self):
        return self.dataframe.to_json(orient='records', default_handler=str)

class DataPoint:
    def __init__(self, date, tou1, tou2, tou3, onCost, midCost, offCost):
        self.date = date
        self.onPeak = tou1
        self.midPeak = tou2
        self.offPeak = tou3
        self.onCost = onCost
        self.midCost = midCost
        self.offCost = offCost

def get_data(xml_file):
    usagePoint = parse.parse_feed(xml_file)[0]
    meterReading = list(usagePoint.meterReadings)[0]
    intervalBlocks = meterReading.intervalBlocks
    onPeak, midPeak, offPeak = [], [], []
    for intervalBlock in intervalBlocks:
        for intervalReading in intervalBlock.intervalReadings:
            if intervalReading.tou == 3:
                offPeak.append(intervalReading)
            elif intervalReading.tou == 2:
                midPeak.append(intervalReading)
            elif intervalReading.tou == 1:
                onPeak.append(intervalReading)
            else:
                raise ValueError("Invalid TOU value")

    dataPoints = []

    for intervalReading in onPeak:
        value = intervalReading.value/1000
        price = intervalReading.cost
        cost = value*price
        dataPoints.append(DataPoint(intervalReading.timePeriod.start, value, 0, 0, cost, 0, 0))

    for intervalReading in midPeak:
        value = intervalReading.value/1000
        price = intervalReading.cost
        cost = value*price
        dataPoints.append(DataPoint(intervalReading.timePeriod.start, 0, value, 0, 0, cost, 0))

    for intervalReading in offPeak:
        value = intervalReading.value/1000
        price = intervalReading.cost
        cost = value*price
        dataPoints.append(DataPoint(intervalReading.timePeriod.start, 0, 0, value, 0, 0, cost))
    
    data_dict = {
        'date': [point.date for point in dataPoints],
        'onPeak': [point.onPeak for point in dataPoints],
        'midPeak': [point.midPeak for point in dataPoints],
        'offPeak': [point.offPeak for point in dataPoints],
        'onCost': [point.onCost for point in dataPoints],
        'midCost': [point.midCost for point in dataPoints],
        'offCost': [point.offCost for point in dataPoints],
    }

    df = pd.DataFrame(data_dict)
    df.sort_values(by='date', inplace=True)
    df = df.reset_index(drop=True)
    df['date'] = df['date'].dt.tz_convert('America/Vancouver')
    df['group_key'] = df['date'].dt.to_period('D')
    df.drop('date', axis=1, inplace=True)
    date_counts = df['group_key'].value_counts()
    valid_dates = date_counts[date_counts > 20].index
    df = df[df['group_key'].isin(valid_dates)]
    grouping = df.groupby('group_key').sum()
    grouping = grouping.reset_index()
    grouping['total'] = (grouping['onPeak'] + grouping['midPeak'] + grouping['offPeak'])
    grouping['totalCost'] = (grouping['onCost'] + grouping['midCost'] + grouping['offCost'])

    return GreenData(grouping)
