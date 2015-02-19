--The script can be run in three ways:
--1. launch hive with the arguments "hive -f <local_path_to>/createTables.hql
--2. within the Hive CLI, run command "source <local_path_to>/createTables.hql;"
--3. the commands from this create script can also be pasted into the Hive CLI manually

--add jar and programs and set some properties
add file /home/local/zslf023/Transform.py;
add jars /home/local/zslf023/hivexmlserde-1.0.5.1.jar;
add jars /home/local/zslf023/brickhouse-0.7.0-SNAPSHOT.jar;

set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.cli.print.current.db=true;
set mapred.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.cli.print.current.db=true;

CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';

--create and use database
DROP DATABASE IF EXISTS pdb10 CASCADE;
CREATE DATABASE IF NOT EXISTS pdb10;
USE pdb10;

--load XML files
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
CREATE TABLE splitPDB AS
SELECT split(pdbid,"-")[0] as pdbid, split(entry,"\\p{Zs}+") as record
FROM pdb_xml_load LATERAL VIEW explode(record) splitTable AS entry
CLUSTER BY pdbid;



--error checking if have correct number of elements in record array and all fields have values
CREATE TABLE error_records
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


--DIHEDRALS-SPECIFIC TABLES
--discards any residues not in list, chooses only N, CA and C atom records
--group coordinates by residue into a map
CREATE TABLE coordinates_grouped AS
SELECT pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id, label_comp_id, collect(label_atom_id,ARRAY(cartnx,cartny,cartnz)) as atoms
FROM atom_coordinates
WHERE label_atom_id in ("N","CA","C")
AND label_comp_id in ('ARG', 'HIS', 'LYS', 'ASP', 'GLU', 'SER', 'THR','ASN', 'GLN', 'CYS', 'GLY', 'PRO', 'ALA', 'ILE', 'LEU', 'MET', 'PHE', 'TRP', 'TYR', 'VAL')
GROUP BY pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id,label_comp_id;

--error checking - need to discard entries that don't have all three atoms in their map
CREATE VIEW noatoms AS 
SELECT DISTINCT pdbid 
FROM coordinates_grouped 
WHERE SIZE(atoms) != 3;

--final table having discarded any entries in noatoms
CREATE TABLE residue_coordinates AS
SELECT  t1.pdbid, t1.pdbx_PDB_model_num, t1.label_asym_id, t1.label_seq_id, t1.label_comp_id, t1.atoms
FROM coordinates_grouped t1
LEFT OUTER JOIN noatoms 
ON (t1.pdbid = noatoms.pdbid) 
WHERE noatoms.pdbid IS NULL;

--make tables publicly accessible (can omit if not necessary)
GRANT SELECT ON TABLE splitpdb TO ROLE public;
GRANT SELECT ON TABLE error_records TO ROLE public;
GRANT SELECT ON TABLE atom_coordinates TO ROLE public;
GRANT SELECT ON VIEW noatoms TO ROLE public;
GRANT SELECT ON TABLE residue_coordinates TO ROLE public;