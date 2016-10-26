-- Load events data from hdfs and parse records into columns
source_events = LOAD '/user/data/*.export.CSV' USING PigStorage('\t') AS (GLOBALEVENTID:chararray,	SQLDATE:chararray,	MonthYear:chararray,	Year:chararray,	FractionDate:chararray,	Actor1Code:chararray,	Actor1Name:chararray,	Actor1CountryCode:chararray,	Actor1KnownGroupCode:chararray,	Actor1EthnicCode:chararray,	Actor1Religion1Code:chararray,	Actor1Religion2Code:chararray,	Actor1Type1Code:chararray,	Actor1Type2Code:chararray,	Actor1Type3Code:chararray,	Actor2Code:chararray,	Actor2Name:chararray,	Actor2CountryCode:chararray,	Actor2KnownGroupCode:chararray,	Actor2EthnicCode:chararray,	Actor2Religion1Code:chararray,	Actor2Religion2Code:chararray,	Actor2Type1Code:chararray,	Actor2Type2Code:chararray,	Actor2Type3Code:chararray,	IsRootEvent:chararray,	EventCode:chararray,	EventBaseCode:chararray,	EventRootCode:chararray,	QuadClass:int,	GoldsteinScale:double,	NumMentions:int,	NumSources:int,	NumArticles:int,	AvgTone:double,	Actor1Geo_Type:chararray,	Actor1Geo_FullName:chararray,	Actor1Geo_CountryCode:chararray,	Actor1Geo_ADM1Code:chararray,	Actor1Geo_Lat:double,	Actor1Geo_Long:double,	Actor1Geo_FeatureID:int,	Actor2Geo_Type:chararray,	Actor2Geo_FullName:chararray,	Actor2Geo_CountryCode:chararray,	Actor2Geo_ADM1Code:chararray,	Actor2Geo_Lat:double,	Actor2Geo_Long:double,	Actor2Geo_FeatureID:int,	ActionGeo_Type:chararray,	ActionGeo_FullName:chararray,	ActionGeo_CountryCode:chararray,	ActionGeo_ADM1Code:chararray,	ActionGeo_Lat:double,	ActionGeo_Long:double,	ActionGeo_FeatureID:int,	DATEADDED:int,	SOURCEURL:chararray);	

-- Tokenize the URL into multiple parts
tokenized_underscore = FOREACH source_events generate GLOBALEVENTID, SOURCEURL,INDEXOF(SOURCEURL, '//',0),INDEXOF(SOURCEURL, '/',2);

-- Group the words
--grouped = GROUP tokenized_underscore by word;

-- Count the words
--counted = FOREACH grouped GENERATE group,COUNT(tokenized_underscore.GLOBALEVENTID);

--ordered = ORDER counted by 
limited = LIMIT tokenized_underscore 10;

-- dump
DUMP limited;

-- Store the output into HDFS
--STORE counted INTO '/user/data/popular_words_in_october.CSV';
