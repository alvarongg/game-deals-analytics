CREATE TABLE IF NOT EXISTS game_deals (
    internalName VARCHAR(255),
    title VARCHAR(255),
    metacriticLink VARCHAR(255),
    dealID INT,
    storeID INT,
    gameID INT,
    salePrice NUMERIC(10,2),
    normalPrice NUMERIC(10,2),
    isOnSale BOOLEAN,
    savings NUMERIC(10,2),
    metacriticScore INT,
    steamRatingText VARCHAR(255),
    steamRatingPercent INT,
    steamRatingCount INT,
    steamAppID INT,
    releaseDate DATE,
    lastChange TIMESTAMP,
    dealRating NUMERIC(10,2),
    thumb VARCHAR(255),
    execution_datetime TIMESTAMP
);