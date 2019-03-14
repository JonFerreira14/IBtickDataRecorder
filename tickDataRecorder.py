"""
    Program checks if we are withing the desired trading hours, then starts a socket connection
    to IB's servers and records tick-by-tick bid/ask spread data to a .csv file
"""

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract

import time
from threading import Thread
import queue

import datetime
import pandas as pd
import numpy as np

## IB server request ids
DEFAULT_MARKET_DATA_ID=1
DEFAULT_GET_CONTRACT_ID=2

## Markers to identify queue status
FINISHED = object()
STARTED = object()
TIME_OUT = object()

class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns every element from a queue until it reaches the end or timesout
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)

            except queue.Empty:
                ## Timeout
                finished = True
                self.status = TIME_OUT


        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


class stream_of_ticks(list):
    """
    Take a stream of ticks and concat them into a pandas datafram
    """

    def __init__(self, list_of_ticks):
        super().__init__(list_of_ticks)

    def as_pdDataFrame(self):

        if len(self)==0:
            ## no data; do a blank tick
            print("nodata")
            return tickObject(datetime.datetime.now()).as_pandas_row()

        pd_row_list=[tick.as_pandas_row() for tick in self]
        pd_data_frame=pd.concat(pd_row_list)

        return pd_data_frame


class tickObject(object):
    """
    Method for storing tick data from IB as pandas rows
    """
    def __init__(self, timestamp, bid_size=np.nan, bid_price=np.nan,
                 ask_size=np.nan, ask_price=np.nan):

        self.timestamp = timestamp
        self.bid_size = bid_size
        self.bid_price = bid_price
        self.ask_size = ask_size
        self.ask_price = ask_price

    def __repr__(self):
        return self.as_pandas_row().__repr__()

    def as_pandas_row(self):
        attributes=['bid_size','bid_price', 'ask_size', 'ask_price']
        ## Create dict with attributes to pass to pandas
        self_as_dict=dict([(attr_name, getattr(self, attr_name)) for attr_name in attributes])
        ## Add to row and use timestamp as index
        return pd.DataFrame(self_as_dict, index=[self.timestamp])


class WrapperObject(EWrapper):
    """
    Wrapper object, recieves data from IB servers, and calls wrapper functions. 
    We overide the default functions here to provide our own functionality.
    """

    def __init__(self):
        self._my_contract_details = {}
        self._my_market_data_dict = {}

    ## Error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)


    ## Get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()
        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## Overridden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)
        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## Overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)
        self._my_contract_details[reqId].put(FINISHED)

    ## Market data
    def init_market_data(self, tickerid):
        ## Creates queue object in wrapper
        market_data_queue = self._my_market_data_dict[tickerid] = queue.Queue()
        return market_data_queue

    def tickByTickBidAsk(self, tickerid, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
        ## Overriden method
        ## Create tick object and put in the queue
        this_tick_data=tickObject(time, bidSize, bidPrice, askPrice, askSize)
        self._my_market_data_dict[tickerid].put(this_tick_data)


class ClientObject(EClient):
    """
    Client object, used to send requests to IB
    """

    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        ## Dict to hold queue object for requesting amrket data
        self._market_data_q_dict = {}

    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        Returns full contract object from given parameters
        """

        ## Make a queue to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract or timeout
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details)==0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details)>1:
            print("got multiple contracts using first one")

        new_contract_details=new_contract_details[0]

        resolved_ibcontract=new_contract_details.contract

        return resolved_ibcontract

    def start_getting_IB_market_data(self, resolved_ibcontract, tickerid=DEFAULT_MARKET_DATA_ID):
        """
        Start socket connection and start streaming tick data
        """
        ## To the client dict, create queue, which is also in wrapper dict
        self._market_data_q_dict[tickerid] = self.wrapper.init_market_data(tickerid)
        ## Start stream
        self.reqTickByTickData(tickerid, resolved_ibcontract, 'BidAsk', 0, False)
        return tickerid

    def stop_getting_IB_market_data(self, tickerid):
        """
        Stops streaming of market data
        """

        ## native EClient method
        self.cancelTickByTickData(tickerid)

        ## Wait time incase of ticks recieved while canceling the connection
        time.sleep(5)

        ## Get remaning market data in queue
        market_data = self.get_IB_market_data(tickerid)

        ## Output any errors
        while self.wrapper.is_error():
            print(self.get_error())

        return market_data

    def get_IB_market_data(self, tickerid):
        """
        Retrieve available market data from queue
        """

        ## How long to wait for next item
        MAX_WAIT_MARKETDATEITEM = 5
        ## Queue object
        market_data_q = self._market_data_q_dict[tickerid]

        market_data=[]
        finished=False        
        while not finished:   
            try:
                market_data.append(market_data_q.get(timeout=MAX_WAIT_MARKETDATEITEM))
                if market_data_q.qsize() ==0:
                    finished=True
            except queue.Empty:
                ## no more data
                finished=True


        return stream_of_ticks(market_data)


class AppObject(WrapperObject, ClientObject):
    '''
    App object, contains wrapper object and client object, sets thread for listening to responses
    from IB
    '''
    def __init__(self, ipaddress, portid, clientid):
        WrapperObject.__init__(self)
        ClientObject.__init__(self, wrapper=self)
        
        self.init_error()
        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)


if __name__ == '__main__':
    ## Recording params
    ## Record from 7am - 4:10pm
    startTime = "07:00:00.0"
    endTime = "16:10:00.0"
    ## Write data every x seconds
    logInterval = 5
    ## File name
    outputName = datetime.datetime.today().strftime('%Y-%m-%d') + ".csv"


    app = AppObject("127.0.0.1", 7497, 999)

    ## Contract specifications
    ibcontract = IBcontract()
    ibcontract.secType = "FUT"
    ibcontract.lastTradeDateOrContractMonth="201903"
    ibcontract.symbol="ES"
    ibcontract.exchange="GLOBEX"

    ## Get full contract object from IB
    resolved_ibcontract = app.resolve_ib_contract(ibcontract)

    ## Wait for recording start time
    print("Waiting for recording period to begin...")
    while datetime.datetime.strptime(startTime, "%H:%M:%S.%f").time() > datetime.datetime.today().time():
        time.sleep(1)
    print("Recording period started")
    ## Start market data connection
    tickerid = app.start_getting_IB_market_data(resolved_ibcontract)

    ## Initial write to csv
    time.sleep(5)
    print("Writing initial entry to file...")
    initialWrite = app.get_IB_market_data(tickerid).as_pdDataFrame()
    print(initialWrite)
    initialWrite.to_csv(outputName)
    print("Initial write completed")

    ## Log Data until we reach end of recording interval
    print("Beginning data logging...")
    with open(outputName, 'a') as targetOutput:
        while datetime.datetime.strptime(endTime, "%H:%M:%S.%f").time() > datetime.datetime.today().time():
            time.sleep(logInterval)
            ## Get current queue data
            temp_market_data = app.get_IB_market_data(tickerid).as_pdDataFrame()
            print(temp_market_data)

            ## Write to file
            temp_market_data.to_csv(targetOutput, header=False)

        print("Streaming complete, logging final qoutes...")
        ## Stops the stream and returns all the data we've got so far
        final_market_data = app.stop_getting_IB_market_data(tickerid).as_pdDataFrame()
        final_market_data.to_csv(targetOutput, header=False)
        print("All data logged, disconnecting...")

    app.disconnect()

