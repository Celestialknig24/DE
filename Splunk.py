def Splunk_query_Execution(Search_query, STime, ETime, batch_size=50000):
    '''Send the result of the Splunk Cloud Query in the format of DataFrame, fetching results in batches.'''
    
    kwargs_search = {
        "earliest_time": STime,
        "latest_time": ETime,
        "count": batch_size,  # Number of records per batch
        "offset": 0  # Starting point for each batch
    }
    
    data = []
    while True:
        job = service.jobs.create(Search_query, **kwargs_search)
        
        while not job.is_done():
            pass
        
        search_results = job.results(count=batch_size, offset=kwargs_search['offset'])
        
        batch_data = [result for result in results.ResultsReader(search_results)]
        
        if not batch_data:
            break
        
        data.extend(batch_data)
        
        # Update offset for next batch
        kwargs_search['offset'] += batch_size

        job.cancel()
    
    df = pl.DataFrame(data)
    return df
