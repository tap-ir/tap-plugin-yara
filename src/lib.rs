//! Scan file through yara 

use std::fmt::Debug;
use std::io::Read;

use tap::config_schema;
use tap::plugin;
use tap::tree::{TreeNodeId, TreeNodeIdSchema};
use tap::plugin::{PluginInfo, PluginInstance, PluginConfig, PluginArgument, PluginResult, PluginEnvironment};
use tap::error::RustructError;

use serde::{Serialize, Deserialize};
use schemars::{JsonSchema};
use yara::Compiler;

plugin!("yara", "Malware", "Scan file content with YARA", YaraPlugin, Arguments);

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Arguments
{
  //take vec of tree node id ?
  #[schemars(with = "TreeNodeIdSchema")] 
  file : TreeNodeId, 
  rules : String,
}

#[derive(Debug, Serialize, Deserialize,Default)]
pub struct Results
{
}

#[derive(Default)]
pub struct YaraPlugin
{
}

impl YaraPlugin
{
  fn run(&mut self, args : Arguments, env : PluginEnvironment) -> anyhow::Result<Results>
  {
    let parent_node = env.tree.get_node_from_id(args.file).ok_or(RustructError::ArgumentNotFound("file"))?;
    parent_node.value().add_attribute(self.name(), None, None); 
    let value = parent_node.value().get_value("data").ok_or(RustructError::ValueNotFound("data"))?;
    let builder = value.try_as_vfile_builder().ok_or(RustructError::ValueTypeMismatch)?;
    let mut file = builder.open()?;
    
    println!("launching yara");

    //we will do that for each file better launch in // or do that one time in a 'global'
    //then run for every file or it will be very slow !
    let compiler = Compiler::new().unwrap();
    let compiler = compiler.add_rules_str(&args.rules).expect("Should have parsed rule");
    let rules = compiler.compile_rules().expect("Should have compiled rules");

    //read file to analyze content
    //XXX must limit file size because it's read in memory ...
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let results = rules.scan_mem(&buffer, buffer.len() as i32).expect("Should have scanned");
    println!("yara found {} results", results.len());
    //XXX add results to node !

    Ok(Results{})
  }
}

