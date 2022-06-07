//! S3 plugin let you load file from your filesystem in rustruct tree

use std::io::Read; 
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::{Arc};

use tap::config_schema;
use tap::plugin;
use tap::plugin::{PluginInfo, PluginInstance, PluginConfig, PluginArgument, PluginResult, PluginEnvironment};
use tap::vfile::{VFile, VFileBuilder};
use tap::tree::{Tree,TreeNodeIdSchema};
use tap::error::RustructError;
use tap::node::Node;

use tap::tree::TreeNodeId;
use tap::value::Value;

use serde::{Serialize, Deserialize};
use schemars::{JsonSchema};
use log::{warn}; 

use rusoto_core::Region;
use rusoto_s3::{S3Client, ListObjectsV2Request, GetObjectRequest, GetObjectOutput};
use rusoto_s3::S3 as S3api;

plugin!("s3", "Input", "Load files from a s3 server", S3, Arguments);

#[derive(Default)]
pub struct S3 
{
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Arguments
{
  bucket : Option<String>,
  address : String,
  key : String,
  secret_key : String,
  #[schemars(with = "TreeNodeIdSchema")] 
  mount_point : TreeNodeId,
}

#[derive(Debug, Serialize, Deserialize,Default)]
pub struct Results
{
}

#[derive(Debug)]
pub struct BucketObject
{
  pub key : String,
  pub size : i64,
}

#[derive(Debug)]
pub struct Bucket
{
  pub name : String,
  pub objects : Vec<BucketObject>
}

impl Bucket
{
  pub fn new(name : &str) -> Self
  {
    Bucket{ name : name.into(),  objects : Vec::new() }
  }
}

pub fn get_buckets() -> Vec<Bucket> //create a list of bucket & files, then it will be used to create the node tree
{
  // must used passe parameter to connect to the endpoint
  let region = Region::Custom{ name : "docker".to_owned(), endpoint : "http://127.0.0.1:9000".to_owned() };
  let client = S3Client::new(region);

  let mut buckets : Vec<Bucket> = Vec::new();

  let buckets_info = client.list_buckets().sync().unwrap();

  for bucket in buckets_info.buckets.unwrap()
  {
    if let Some(bucket_name) = bucket.name
    {
      let mut wrapped_bucket = Bucket::new(&bucket_name);

      let input : ListObjectsV2Request = ListObjectsV2Request{ bucket : bucket_name, continuation_token : None, delimiter : None, encoding_type : None, fetch_owner : None, max_keys : None, prefix : None, request_payer : None, start_after : None }; 
      let list_object_v2_output = client.list_objects_v2(input).sync().unwrap(); 

      for object in list_object_v2_output.contents.unwrap()
      {
        let bucket_object = BucketObject{ key : object.key.unwrap(), size : object.size.unwrap()};
        wrapped_bucket.objects.push(bucket_object);
      }

      buckets.push(wrapped_bucket);
    }
  }
  buckets
}

impl S3 
{
  fn create_bucket_node(&self, bucket: &Bucket) -> Option<Node>
  {
     let node = Node::new(bucket.name.clone());
     node.value().add_attribute("directory", None, None);
     node.value().add_attribute("s3-bucket", None, None);

     Some(node)
  }

  fn create_object_node(&self, bucket : &Bucket, object : &BucketObject) -> Option<Node>
  {
     let node = Node::new(object.key.clone());
     node.value().add_attribute("s3-object", None, None);
     node.value().add_attribute("size", object.size, None);

     let vfile_builder = match S3VFileBuilder::new(bucket, object)
     {
         Ok(vfile_builder) => vfile_builder,
         Err(_) => return None,
     };
     node.value().add_attribute("data", Value::VFileBuilder(Arc::new(vfile_builder)), None);
     node.value().add_attribute("file", None, None);

     Some(node)
  }

  fn creates(&self, parent_id : TreeNodeId, buckets : Vec<Bucket>, tree : &Tree) -> anyhow::Result<()>
  {
    for bucket in buckets.iter()
    {
      if let Some(bucket_node) = self.create_bucket_node(bucket)
      {
         let bucket_id = tree.add_child(parent_id, bucket_node)?;

         for object in bucket.objects.iter()
         {
           if let Some(object_node) = self.create_object_node(bucket, object)
           {
             tree.add_child(bucket_id, object_node)?;
           }
         }
      }
    }
    Ok(())
  }

  fn run(&mut self, args : Arguments, env : PluginEnvironment) -> anyhow::Result<Results>
  {
    warn!("{:?}", args); 
    let buckets = get_buckets();
    self.creates(args.mount_point, buckets, &env.tree)?;

    Ok(Results{})
  }
}

#[derive(Serialize, Deserialize)]
pub struct S3VFileBuilder
{
  bucket : String,
  key : String,
  size : u64,
}

impl S3VFileBuilder
{
  pub fn new(bucket : &Bucket, object : &BucketObject) -> anyhow::Result<S3VFileBuilder>
  {
    Ok(S3VFileBuilder{ bucket : bucket.name.clone(), key : object.key.clone(), size : object.size as u64 })
  }
}

#[typetag::serde]
impl VFileBuilder for S3VFileBuilder
{
  fn open(&self) -> anyhow::Result<Box<dyn VFile>>
  {
    match S3VFile::new(self.bucket.clone(), self.key.clone(), self.size)
    {
      Ok(file) => Ok(Box::new(file)),
      Err(_) => Err(RustructError::OpenFile("Can't open S3VFile".to_string()).into()), 
    }
  }

  fn size(&self) -> u64
  { 
    self.size
  }
}

pub struct S3VFile
{
  pub bucket : String,
  pub key : String,
  pub size : u64,
  pub pos : u64,
}

impl S3VFile
{
  pub fn new(bucket : String, key : String, size : u64) -> anyhow::Result<Self>
  {
    Ok(S3VFile{ bucket, key, size, pos : 0 })
  }
}

pub fn read_object(bucket: &str, key : &str, offset : u64, to_read : usize, size : u64) -> anyhow::Result<GetObjectOutput> 
{
  //this must be constructed from argument
  let region = Region::Custom{ name : "docker".to_owned(), endpoint : "http://127.0.0.1:9000".to_owned() };
  let client = S3Client::new(region);

  //calculate the offset 
  let range_start = offset;
  let mut range_end = offset + to_read as u64;
  if range_end > size
  {
    range_end = size;
  }
  let range_string = format!("bytes={}-{}", range_start, range_end); 

  let request = GetObjectRequest{bucket : bucket.to_string(), 
                                 if_match : None,
                                 if_modified_since : None,  
                                 if_none_match : None,  
                                 if_unmodified_since : None,  
                                 key : key.to_string(),  
                                 part_number : None, 
                                 range : Some(range_string), //this is the offset
                                 request_payer : None,  
                                 response_cache_control : None,  
                                 response_content_disposition : None,  
                                 response_content_encoding : None,  
                                 response_content_language : None,  
                                 response_content_type : None,  
                                 response_expires : None,  
                                 sse_customer_algorithm : None,  
                                 sse_customer_key : None,  
                                 sse_customer_key_md5 : None,  
                                 version_id : None,  
                                 };
 
  match client.get_object(request).sync()
  {
    Ok(get_object_output) => Ok(get_object_output),
    Err(err) => Err(RustructError::Unknown(err.to_string()).into())
  }
}

impl Read for S3VFile 
{
  /// `Read` implem of `S3VFile`
  //S3 can return a Read impl but not Read+Seek
  fn read(&mut self, buf : &mut [u8]) -> std::io::Result<usize>
  {
    let get_object_output = match read_object(&self.bucket, &self.key, self.pos, buf.len(), self.size)
    {
      Ok(get_object_output) => get_object_output,
      Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::Other, err)) //create a conversion func ?
    };
    if let Some(body) = get_object_output.body {
      match body.into_blocking_read().read(buf)
      {
        Ok(size) => 
        {
          self.pos += size as u64; //we update our internal offset
          Ok(size)
        },
        Err(err) => Err(err),
      }
    }
    else {
      Err(std::io::Error::new(std::io::ErrorKind::Other, "Can't get object")) 
    }
  }
}

impl Seek for S3VFile 
{
  /// `Seek` implem of `S3VFile`
  //We handle seek ourselves
  fn seek(&mut self, pos : SeekFrom) -> std::io::Result<u64>
  {
    let pos : u64 = match pos 
    {
      SeekFrom::Start(pos) => pos,
      SeekFrom::End(_pos) =>  return Err(std::io::Error::new(std::io::ErrorKind::Other, "MergeFile::Seek Can't seek past end of file")),//we don't support this as we're not writable, or pos must be 0 to seek to end 
      SeekFrom::Current(pos) => (pos + self.pos as i64) as u64,
    };

    if pos <= self.size
    {
      self.pos = pos;
      return Ok(self.pos);
    }
    Err(std::io::Error::new(std::io::ErrorKind::Other, "MergeFile::Seek Can't seek past maximum size"))
  }
}
