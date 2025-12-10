use std::{
    fs,
    collections::{BTreeMap, BTreeSet},
};
use redis::Value::BulkString;
use clap::Parser;

// CLI
#[derive(Parser, Debug)]
struct Cli {
    #[arg(short)]
    path: String,    

    #[arg(short)]
    size: u32,    
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {    
    let cli = Cli::parse();
    if cli.size != 2 && cli.size != 6 {
        anyhow::bail!("Test size must be `2` or `6`.");
    }

    // setup redis
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    // let redis_client = redis::Client::open("redis://:redispassword@localhost:6379/0")?;    
    let redis_con = redis_client.get_multiplexed_async_connection().await?;

    // write_blocks(redis_con.clone(), cli.path, cli.size).await?;
    read_blocks(redis_con.clone()).await?;

    Ok(())
}

#[allow(unused)]
async fn write_blocks(
    mut redis_con: redis::aio::MultiplexedConnection,
    path: String,
    size: u32,
) -> anyhow::Result<()> {
    let base_blob_path = format!("{}/{}x", path, size);
    let mut subblocks: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    let _: () = redis::cmd("DEL")
        .arg("blocks")
        .exec_async(&mut redis_con)
        .await?;
    for entry in fs::read_dir(&base_blob_path)? {
        let entry = entry?;
        let block_path = entry.path();
        // println!("Reading `{}`", block_path.display());        
        let mut block_entries = block_path.read_dir()?;        
        let mut subblock_entry = block_entries.skip_while(|e|
            !e.as_ref().unwrap().path().is_dir()
        );
        // subblock stdins
        let subblocks_path = subblock_entry.next().unwrap()?.path();
        let num_subblocks = subblocks_path.read_dir()?.count();        
        let block_number = block_path.file_name().unwrap().to_str().unwrap().to_owned();
        let base_subblock_path = format!("{base_blob_path}/{block_number}/subblock_stdins");
        // data layout: (block_number-index, blob)
        print!("block: {block_number}: ");
        for index in 0..num_subblocks {
            let blob = fs::read(format!("{base_subblock_path}/{index}.bin"))?;
            print!("{:?}, ", blob.len());
            let _: () = redis::cmd("XADD")
                .arg("blocks")
                .arg("*")
                .arg(&[(format!("{block_number}-{index}"), blob)])
                .query_async(&mut redis_con).await?;
        }
        // agg stdin is the last item
        let blob = fs::read(format!("{base_blob_path}/{block_number}/agg_stdin.bin"))?;
        println!("{:?}", blob.len());
        let _: () = redis::cmd("XADD")
            .arg("blocks")
            .arg("*")
            .arg(&[(format!("{block_number}-{num_subblocks}"), blob)])
            .query_async(&mut redis_con).await?;            
        // break;
    }    

    Ok(())
}

#[allow(unused)]
async fn read_blocks(
    mut redis_con:redis::aio::MultiplexedConnection,
) -> anyhow::Result<()> {
    let mut last_id = "0";
    let result: redis::Value = redis::cmd("XREAD")
        .arg("BLOCK").arg(0)
        .arg("STREAMS").arg("blocks").arg(last_id)
        .query_async(&mut redis_con)
        .await
        .unwrap();        
    let streams = result.as_sequence().unwrap();
    let contents = streams[0].as_sequence().unwrap();
    let _stream_name = &contents[0];
    let entries = contents[1].as_sequence().unwrap();
    for entry in entries {
        let items = &entry.as_sequence().unwrap();
        let _id = &items[0];
        if let BulkString(bs) = &items[1].as_sequence().unwrap()[0] {
            let blob_id = String::from_utf8_lossy(&bs).into_owned();
            // let tokens: Vec<_> = s.split("-").collect();
            print!("{blob_id:?}: ");
        }
        if let BulkString(bs) = &items[1].as_sequence().unwrap()[1] {
            let blob = bs;
            println!("{:?}", blob.len());
        }
    }
    
    Ok(())
}