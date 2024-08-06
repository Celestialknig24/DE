import concurrent.futures
import polars as pl

def get_total_record_count(Search_query, STime, ETime):
    '''Query Splunk to get the total number of records for a given query.'''
    kwargs_search = {
        "earliest_time": STime,
        "latest_time": ETime,
        "count": 1  # We only need a count, not the actual data
    }
    
    job = service.jobs.create(Search_query, **kwargs_search)
    
    while not job.is_done():
        pass
    
    search_results = job.results(count=1)
    
    # Assuming the job results include a field for total records (adjust as needed)
    total_count = int(job["resultCount"])
    
    job.cancel()
    
    return total_count

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
    
    # Get the total number of records dynamically
    total_records = get_total_record_count(Search_query, STime, ETime)
    
    # Calculate the number of batches needed
    estimated_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
    
    offsets = [i * batch_size for i in range(estimated_batches)]
    
    data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [executor.submit(fetch_batch, Search_query, STime, ETime, batch_size, offset) for offset in offsets]
        
        for future in concurrent.futures.as_completed(futures):
            batch_data = future.result()
            if batch_data:
                data.extend(batch_data)
    
    df = pl.DataFrame(data)
    return df
