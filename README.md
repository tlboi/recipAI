# recipAI
The end-goal of this project is to retrieve enough recipes from the web to obtain a recipe database to train an AI language model to cook and come up with its own original recipes! So far we have 7 million recipes scraped from the web. 

1. Ready a .txt file with all your main domain URLs
2. Run robot.py to establish the list of websites that allow crawling
3. Run crawler.py to get all the recipe-related sub-urls
4. Run filter.py to discard those that are potentially not recipes
5. Run fetcher.py to get light-weight htmls
6. Run parser.py to get recipe-relevant entries into clean database files
