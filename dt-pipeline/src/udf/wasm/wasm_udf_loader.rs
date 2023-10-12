use dt_common::error::Error;
use dt_meta::row_data::RowData;
use serde_json;
use std::fs;
use wasmer::{Instance, Memory, Module, Store, Value};
use wasmer_wasix::{WasiEnvBuilder, WasiFunctionEnv};

static WASM_PROGRAM_NAME: &str = "cubetran_udf";
static WASM_WORK_FUNC: &str = "WorkWithData";
static WASM_FREEMEM_FUNC: &str = "FreeMemory";
pub struct WasmUdfLoader {
    pub store: Option<Store>,
    pub instance: Option<Instance>,
    pub wasi_env: Option<WasiFunctionEnv>,
}

impl WasmUdfLoader {
    pub fn init_instance(&mut self, wasm_plugin: String) {
        if wasm_plugin.is_empty() {
            return;
        }

        let wasm_bytes = fs::read(&wasm_plugin)
            .expect(format!("failed to load wasm file:[{}]", &wasm_plugin).as_str());
        let mut store = Store::default();
        let module = Module::new(&store, wasm_bytes).unwrap();

        let wasi_env_builder = WasiEnvBuilder::new(WASM_PROGRAM_NAME);
        let wasi_env = wasi_env_builder.finalize(&mut store).unwrap();
        let import_object = wasi_env.import_object(&mut store, &module).unwrap();

        let new_instance = Instance::new(&mut store, &module, &import_object).unwrap();

        self.store = Some(store);
        self.instance = Some(new_instance);
        self.wasi_env = Some(wasi_env);
    }

    pub fn work_with_data(&mut self, data: RowData) -> Result<RowData, Error> {
        let (store, instance, wasi_env) = self.get_basic_info()?;

        let work_func = instance.exports.get_function(WASM_WORK_FUNC).unwrap();
        let datas_str = serde_json::to_string(&data).unwrap();
        let data_bytes = datas_str.as_bytes();

        let memory = instance.exports.get_memory("memory").unwrap();
        let memory_view = memory.view(&store);
        memory_view.write(0, data_bytes).unwrap();

        let result = work_func
            .call(store, &[Value::I32(0), Value::I32(data_bytes.len() as i32)])
            .unwrap();

        let result_ptr = result[0].unwrap_i32() as usize;
        let result_str =
            WasmUdfLoader::read_string_from_memory(memory, &store, result_ptr).unwrap();

        let result_data: RowData = serde_json::from_str(result_str.as_str()).unwrap();

        WasmUdfLoader::wasm_free_mem(store, instance, wasi_env, result_ptr)?;

        return Ok(result_data);
    }

    fn wasm_free_mem(
        store: &mut Store,
        instance: &Instance,
        wasi_env: &WasiFunctionEnv,
        ptr: usize,
    ) -> Result<(), Error> {
        let free_func = instance.exports.get_function(WASM_FREEMEM_FUNC).unwrap();
        free_func.call(store, &[Value::I32(ptr as i32)]).unwrap();

        wasi_env.cleanup(store, None);

        Ok(())
    }

    fn get_basic_info(&mut self) -> Result<(&mut Store, &Instance, &WasiFunctionEnv), Error> {
        let store = if let Some(i) = &mut self.store {
            i
        } else {
            return Err(Error::UdfError("wasm udf store is not init".to_string()));
        };
        let instance = if let Some(i) = &self.instance {
            i
        } else {
            return Err(Error::UdfError("wasm udf instance is not init".to_string()));
        };
        let wasi_env = if let Some(we) = &self.wasi_env {
            we
        } else {
            return Err(Error::UdfError("wasm udf wasi env is not init".to_string()));
        };

        Ok((store, instance, wasi_env))
    }

    fn read_string_from_memory(
        memory: &Memory,
        store: &Store,
        ptr: usize,
    ) -> Result<String, Error> {
        let memory_view = memory.view(store);

        let mut result = Vec::new();
        let mut offset = ptr;
        loop {
            let byte = memory_view.read_u8(offset as u64).unwrap();
            if byte == 0 {
                break;
            }
            result.push(byte);
            offset += 1;
        }

        Ok(String::from_utf8(result)?)
    }
}
