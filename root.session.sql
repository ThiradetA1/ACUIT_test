CREATE TABLE chicago_crimes (
    id INT PRIMARY KEY,
    case_number VARCHAR(20),
    occurrence_date DATETIME,
    primary_type VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    location POINT SRID 4326
);

CREATE INDEX idx_crime_date ON chicago_crimes (occurrence_date);
CREATE SPATIAL INDEX idx_crime_location ON chicago_crimes (location);