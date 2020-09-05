use foundationdb::tuple::{pack, unpack, Subspace};
use read_byte_slice::{ByteSliceIter, FallibleStreamingIterator};
use foundationdb::{Database, RangeOption, KeySelector};
use std::borrow::Cow;

use std::io::Write;
use std::env;
const CHUNK_SIZE: usize = 10000;

async fn async_main() -> foundationdb::FdbResult<()> {
    let args: Vec<String> = env::args().collect();

    let method = &args[1];

    let source = args[2].to_string();

    let db = foundationdb::Database::default()?;
    println!("Method: {}", method);
    match method.as_str() {
        "get" => get(db, source, args[3].to_string()).await?,
        "set" => set(db, source, args[3].to_string()).await?,
        "del" => del(db, source).await?,
        _ => println!("Use get or set <source> <target>")
    }
    Ok(())
}

async fn get(db: Database, source: String, target: String) -> foundationdb::FdbResult<()> {
    let trx = db.create_trx()?;
    let mut file = std::fs::File::create(&target).expect("Could not open file");
    let subspace = Subspace::from("files");

    let tuple = subspace.subspace(&(source)).range();
    let range = RangeOption::from(tuple);

    for key_value in trx.get_range(&range, 1, false).await?.iter() {
        let bytes = unpack::<Vec<u8>>(key_value.value()).unwrap();
        file.write(&bytes).expect("Could not write file");
    }
    Ok(())
}

async fn del(db: Database, source: String) -> foundationdb::FdbResult<()> {
    let trx = db.create_trx()?;
    let subspace = Subspace::from("files");

    let (start, end) = subspace.subspace(&(source)).range();

    trx.clear_range(&start, &end);
    trx.commit().await?;

    Ok(())
}

async fn set(db: Database, source: String, target: String) -> foundationdb::FdbResult<()> {
    let file = std::fs::File::open(&source).expect("Could not open file");
    let mut iter: i32 = 0;
    let subspace = Subspace::from("files");
    let mut chunks = ByteSliceIter::new(file, CHUNK_SIZE);

    while let Some(chunk) = chunks.next().expect("Broken") {
        let trx = db.create_trx()?;
        let key = subspace.pack(&(&target, iter));

        trx.set(&key, &pack(&chunk));
        trx.commit().await?;

        iter += 1;
    }

    Ok(())

}
fn main()  {
    let network = foundationdb::boot().expect("failed to initialize Fdb");
    futures::executor::block_on(async_main()).expect("failed to run");
    // cleanly shutdown the client
    drop(network);

}
