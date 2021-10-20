-- service DB
ALTER TABLE news ADD CONSTRAINT news_unique UNIQUE(news_id);