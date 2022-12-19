use std::sync::Arc;
use arrow::error::Result as ArrowResult;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use itertools::Itertools;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::MetadataVersion;
use arrow::ipc::CompressionType;

fn main() -> ArrowResult<()> {

    // create dict by encoding an one-record batch
    let sample_array0: ArrayRef = Arc::new(StringArray::from_iter_values([""]));
    let sample_array1: ArrayRef = Arc::new(BooleanArray::from_iter([Some(false)]));
    let sample_array2: ArrayRef = Arc::new(Int8Array::from_iter_values([0]));
    let sample_array3: ArrayRef = Arc::new(Int16Array::from_iter_values([0]));
    let sample_array4: ArrayRef = Arc::new(Int32Array::from_iter_values([0]));
    let sample_array5: ArrayRef = Arc::new(Int64Array::from_iter_values([0]));
    let sample_array6: ArrayRef = Arc::new(UInt8Array::from_iter_values([0]));
    let sample_array7: ArrayRef = Arc::new(UInt16Array::from_iter_values([0]));
    let sample_array8: ArrayRef = Arc::new(UInt32Array::from_iter_values([0]));
    let sample_array9: ArrayRef = Arc::new(UInt64Array::from_iter_values([0]));
    let sample_batch = RecordBatch::try_from_iter_with_nullable([
        ("", sample_array0.clone(), false),
        ("", sample_array1.clone(), false),
        ("", sample_array2.clone(), false),
        ("", sample_array3.clone(), false),
        ("", sample_array4.clone(), false),
        ("", sample_array5.clone(), false),
        ("", sample_array6.clone(), false),
        ("", sample_array7.clone(), false),
        ("", sample_array8.clone(), false),
        ("", sample_array9.clone(), false),
        ("", sample_array0.clone(), true),
        ("", sample_array1.clone(), true),
        ("", sample_array2.clone(), true),
        ("", sample_array3.clone(), true),
        ("", sample_array4.clone(), true),
        ("", sample_array5.clone(), true),
        ("", sample_array6.clone(), true),
        ("", sample_array7.clone(), true),
        ("", sample_array8.clone(), true),
        ("", sample_array9.clone(), true),
    ])?;
    let mut dict_buf = vec![];
    let mut writer = StreamWriter::try_new(
        &mut dict_buf,
        &sample_batch.schema(),
    )?;
    writer.write(&sample_batch)?;
    writer.finish()?;
    drop(writer);

    println!("dict:\n{}", dict_buf
        .iter()
        .chunks(8)
        .into_iter()
        .map(|chunk| chunk.into_iter().map(|b| format!("{:#04x}", b)).join(", "))
        .join(",\n"));
    println!("dict_size: {}", dict_buf.len());
    println!("");

    // prepare test batch
    let array1: ArrayRef = Arc::new(StringArray::from_iter_values((0..100000).into_iter().map(|_| "20220101".to_owned())));
    //let array2: ArrayRef = Arc::new(Int32Array::from_iter_values([123456789i32]));
    //let array3: ArrayRef = Arc::new(Int64Array::from_iter_values([123456789123456789i64]));
    //let array4: ArrayRef = Arc::new(Int32Array::from_iter([None]));
    //let array5: ArrayRef = Arc::new(Int64Array::from_iter([None]));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("", array1, false),
        //("", array2, false),
        //("", array3, false),
        //("", array4, true),
        //("", array5, true),
    ])?;

    // test
    let level = 1;
    let mut buf1: Vec<u8> = vec![];
    //let zwriter = zstd::Encoder::new(&mut buf1, level)?
    //    .auto_finish();
    let mut f = std::fs::File::create("./test.arrow")?;
    let mut writer = StreamWriter::try_new_with_options(&mut f, &batch.schema(), IpcWriteOptions::try_new(64, false, MetadataVersion::V5)?.try_with_compression(None)?)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);

    let mut buf2 = vec![];
    let dict = zstd::dict::EncoderDictionary::copy(&dict_buf, level);
    let zwriter = zstd::Encoder::with_prepared_dictionary(&mut buf2, &dict)?
        .auto_finish();
    let mut writer = StreamWriter::try_new(zwriter, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    println!("test batch_mem_size:                {}", batch.get_array_memory_size());
    println!("test batch_ipc_size (without dict): {}", buf1.len());
    println!("test batch_ipc_size (with dict):    {}", buf2.len());
    Ok(())
}
