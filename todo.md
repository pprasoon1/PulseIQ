3. Two Small Problems to Fix
Your logs also show two minor issues (not crashes) that we should fix for performance:

Twitter Rate Limit: Error scraping Twitter for topic '...': 429 Too Many Requests. This means we are hitting the Twitter API too hard, too fast, and they are temporarily blocking us. This is expected. We need to be gentler.

Mongo Slow Query: pulseiq-mongo ... Slow query ... "update":"raw_posts". The database is warning us that our upsert command is slow. This is because it has to scan the whole collection to find the source_id to update.

4. Geolocation tagging of dataset