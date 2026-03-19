#!/bin/zsh
cd /Users/bytedance/Documents/projects/vibe/propertyclaude && \
.venv/bin/python3 scraper.py \
  --url "https://www.zoopla.co.uk/for-sale/houses/kt2-6rl/?baths_min=1&beds_min=3&is_auction=false&is_retirement_home=false&is_shared_ownership=false&price_max=1500000&property_sub_type=semi_detached&property_sub_type=detached&property_sub_type=terraced&property_sub_type=bungalow&q=KT2%206RL&radius=5&search_source=for-sale&tenure=freehold" \
  --max-pages 99 \
  --no-headless \
  --gmaps-key "AIzaSyCXRSodNEdUymcWkzu3OPSOrexGg47KR-A" \
  --verbose