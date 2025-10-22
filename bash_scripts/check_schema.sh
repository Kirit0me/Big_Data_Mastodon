#!/bin/bash
# Check current database schema

echo "Current 'posts' table structure:"
echo "================================"
sudo -u postgres psql -d mastodon_analytics -c "\d posts"

echo ""
echo "Columns in 'posts' table:"
echo "========================="
sudo -u postgres psql -d mastodon_analytics -c "
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'posts'
ORDER BY ordinal_position;"

echo ""
echo "Sample data from 'posts' table:"
echo "==============================="
sudo -u postgres psql -d mastodon_analytics -c "SELECT * FROM posts LIMIT 3;"

echo ""
echo "All tables in database:"
echo "======================="
sudo -u postgres psql -d mastodon_analytics -c "\dt"