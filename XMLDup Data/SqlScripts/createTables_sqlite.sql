CREATE TABLE IF NOT EXISTS DBLOAD
(
   ID		INTEGER,
   OBJ_INDEX	INTEGER,
   ATTRIBUTE	VARCHAR(500),
   CONTENT	TEXT,
   OCCURRENCE	INTEGER,
   DATA_TYPE    INTEGER
);

CREATE TABLE IF NOT EXISTS OBJ_KEYS
(
   OBJ_INDEX	INTEGER,
   OBJ_KEY1	VARCHAR(500),
   OBJ_KEY2	VARCHAR(500),
   OBJ_KEY3	VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS DISTANCE
(
   STRING1 	TEXT,
   STRING2      TEXT,
   DISTANCE     DOUBLE
);

CREATE TABLE IF NOT EXISTS CLUSTERS
(
  ATTRIBUTE	TEXT,
  CONTENT	TEXT,		
  DISTANCE	DOUBLE,       
  OCCURRENCES	INTEGER,
  CLUSTER	INTEGER
);

CREATE TABLE IF NOT EXISTS FEATURES_ASSGN
(
  Distinctiveness DOUBLE,
  HarmonicMean DOUBLE,
  StdDeviation DOUBLE,
  DiversityMean DOUBLE,
  DiversityIndex DOUBLE,
  AttributesPerObject FLOAT,
  DistinctivenessEntropy DOUBLE,
  AVGStringSize DOUBLE,		
  StringSizeEntropy DOUBLE,
  Emptiness DOUBLE,    
  NonNumericContentRate DOUBLE,
  NumericContentRate DOUBLE,
  MixedContentRate DOUBLE,
  Attribute TEXT,
  DB TEXT,
  NodeLevel INTEGER,
  RelativeDepth FLOAT
);

CREATE TABLE IF NOT EXISTS FEATURES_ASSGN_KEYS
(
  Distinctiveness DOUBLE,
  Distinctiveness_ST1 DOUBLE,
  Distinctiveness_ST2 DOUBLE,
  Distinctiveness_ST3 DOUBLE,
  Distinctiveness_END1 DOUBLE,
  Distinctiveness_END2 DOUBLE,
  Distinctiveness_END3 DOUBLE,
  HarmonicMean DOUBLE,
  HarmonicMean_ST1 DOUBLE,
  HarmonicMean_ST2 DOUBLE,
  HarmonicMean_ST3 DOUBLE,
  HarmonicMean_END1 DOUBLE,
  HarmonicMean_END2 DOUBLE,
  HarmonicMean_END3 DOUBLE,
  StdDeviation DOUBLE,
  StdDeviation_ST1 DOUBLE,
  StdDeviation_ST2 DOUBLE,
  StdDeviation_ST3 DOUBLE,
  StdDeviation_END1 DOUBLE,
  StdDeviation_END2 DOUBLE,
  StdDeviation_END3 DOUBLE,
  DiversityMean DOUBLE,
  DiversityMean_ST1 DOUBLE,
  DiversityMean_ST2 DOUBLE,
  DiversityMean_ST3 DOUBLE,
  DiversityMean_END1 DOUBLE,
  DiversityMean_END2 DOUBLE,
  DiversityMean_END3 DOUBLE,
  DiversityIndex DOUBLE,
  DiversityIndex_ST1 DOUBLE,
  DiversityIndex_ST2 DOUBLE,
  DiversityIndex_ST3 DOUBLE,
  DiversityIndex_END1 DOUBLE,
  DiversityIndex_END2 DOUBLE,
  DiversityIndex_END3 DOUBLE,
  AttributesPerObject FLOAT,
  Entropy DOUBLE,
  Entropy_ST1 DOUBLE,
  Entropy_ST2 DOUBLE,
  Entropy_ST3 DOUBLE,
  Entropy_END1 DOUBLE,
  Entropy_END2 DOUBLE,
  Entropy_END3 DOUBLE,
  AVGStringSize DOUBLE,		
  StringSizeEntropy DOUBLE,
  Emptiness DOUBLE,    
  NonNumericContentRate DOUBLE,
  NumericContentRate DOUBLE,
  MixedContentRate DOUBLE,
  Attribute TEXT,
  DB TEXT
);

CREATE TABLE IF NOT EXISTS ATTR_STRUCT_LEVELS
(
  Attribute TEXT,
  NodeLevel INTEGER,
  Occurrences INTEGER
);

CREATE TABLE IF NOT EXISTS ATTR_LEVEL_STATS
(
  Attribute TEXT,
  SelectedNodeLevels INTEGER,
  MaxOccurNodeLevel INTEGER,
  MaxOccurPercentage FLOAT,
  OccurrencesEntropy FLOAT,
  OccurrencesStdDeviation FLOAT,
  OccurrencesDiversity FLOAT
);


