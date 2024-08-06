import concurrent.futures
import polars as pl

def fetch_batch(Search_query, STime, ETime, batch_size, offset):
    '''Fetch a batch of records from Splunk starting at a specific offset.'''
    kwargs_search = {
        "earliest_time": STime,
        "latest_time": ETime,
        "count": batch_size,
        "offset": offset
    }
    
    job = service.jobs.create(Search_query, **kwargs_search)
    
    while not job.is_done():
        pass
    
    search_results = job.results(count=batch_size, offset=offset)
    
    batch_data = [result for result in results.ResultsReader(search_results)]
    
    job.cancel()
    
    return batch_data

def Splunk_query_Execution(Search_query, STime, ETime, batch_size=50000, thread_count=32):
    '''Send the result of the Splunk Cloud Query in the format of DataFrame, fetching results in parallel.'''
    
    data = []
    offset = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        while True:
            # Submit a new batch to the executor
            futures.append(executor.submit(fetch_batch, Search_query, STime, ETime, batch_size, offset))
            offset += batch_size
            
            # Check if the last batch returned any data
            if len(futures) >= thread_count:
                # Wait for the oldest future to complete
                future = futures.pop(0)
                batch_data = future.result()
                if batch_data:
                    data.extend(batch_data)
                else:
                    # If no data is returned, stop further processing
                    break
    
    df = pl.DataFrame(data)
    return df
