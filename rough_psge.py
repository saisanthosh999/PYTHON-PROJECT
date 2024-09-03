import timeit
start_time = timeit.default_timer()

lst = ['https://api.spotify.com/v1/search','https://api.spotify.com/v1/search','https://api.spotify.com/v1/search']
for api in lst:
    print(api)

end_time = timeit.default_timer()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")