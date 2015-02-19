add JARS file:///home/local/zslf023/hivexmlserde-1.0.5.1.jar;

--experiment with compressed files: works great!
CREATE EXTERNAL TABLE noatom(pdbid STRING, entryid array<STRING>, method array<STRING>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.entryid"="/*[local-name()='datablock']/*[local-name()='exptlCategory']/*[local-name()='exptl']/@entry_id",
"column.xpath.method"="/*[local-name()='datablock']/*[local-name()='exptlCategory']/*[local-name()='exptl']/@method"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/noatom"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);


--11AS-noatom	["11AS"]	["X-RAY DIFFRACTION"]
--11BA-noatom	["11BA"]	["X-RAY DIFFRACTION"]