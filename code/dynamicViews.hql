--add jar and set some properties
add JARS file:///home/local/zslf023/hivexmlserde-1.0.5.1.jar;
add JARS file:///home/local/zslf023/brickhouse-0.7.0-SNAPSHOT.jar;

set hive.cli.print.current.db=true;
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';

DROP DATABASE IF EXISTS pdb10 CASCADE;
CREATE DATABASE IF NOT EXISTS pdb10;
USE pdb_sample;


--load XML files (not in subdirectories)
CREATE EXTERNAL TABLE pdb_xml_load(pdbid STRING, record ARRAY<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.record"="/*[local-name()='datablock']/*[local-name()='category_atom_record']/*[local-name()='atom_record']/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/pdb/pdb10"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);


--split atom records into string arrays
CREATE VIEW splitPDB AS
SELECT split(pdbid,"-")[0] as pdbid, split(entry,"\\p{Zs}+") as record
FROM pdb_xml_load LATERAL VIEW explode(record) splitTable AS entry
CLUSTER BY pdbid;


--error checking if have correct number of elements in record array and all fields have values
CREATE VIEW error_records
AS
SELECT DISTINCT pdbid from splitpdb WHERE
record[0] != "HETATM" 
AND
(size(record) !=20
OR record[2] = "."
OR record[2] = "?"
OR record[4] = "."
OR record[8] = "."
OR record[8] = "?"
OR record[11] = "."
OR record[11] = "?"
OR record[13] = "."
OR record[13] = "?"
OR record[14] = "."
OR record[14] = "?"
OR record[15] = "."
OR record[15] = "?");


--create coordinates table, discarding HETATM records and malformed records
--using naming convention from XML file/mmCIF format
CREATE TABLE atom_coordinates AS
SELECT splitpdb.pdbid, cast(record[1] as int) as pdbx_PDB_model_num, record[0] as group_pdb, record[11] as label_atom_id, record[8] as label_comp_id, record[2] as label_asym_id, cast(record[4] as int) as label_seq_id, cast(record[13] as double) as CartnX,cast(record[14] as double) as CartnY,cast(record[15] as double) as CartnZ
FROM splitpdb
LEFT OUTER JOIN error_records
ON (splitpdb.pdbid = error_records.pdbid)
WHERE error_records.pdbid IS null
and record[0] = "ATOM"
DISTRIBUTE BY splitpdb.pdbid;


-- CREATE TABLE coordinates_old AS
-- SELECT splitpdb.pdbid, cast(record[1] as int) as pdbx_PDB_model_num, record[2] as label_asym_id, cast(record[4] as int) as label_seq_id, record[8] as label_comp_id, record[11] as label_atom_id, cast(record[13] as double) as CartnX,cast(record[14] as double) as CartnY,cast(record[15] as double) as CartnZ
-- FROM splitpdb
-- LEFT OUTER JOIN error_records
-- ON (splitpdb.pdbid = error_records.pdbid)
-- WHERE error_records.pdbid IS null
-- and record[0] = "ATOM";


-- possible to extend tables with different data structure
CREATE TABLE coordinates_grouped AS
SELECT pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id, label_comp_id, collect(label_atom_id,ARRAY(cartnx,cartny,cartnz)) as atoms
FROM atom_coordinates
WHERE label_atom_id in ("N","CA","C")
GROUP BY pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id,label_comp_id;



GRANT SELECT ON TABLE atom_coordinates TO USER MXBA001;
GRANT SELECT ON TABLE coordinates_grouped TO USER MXBA001;