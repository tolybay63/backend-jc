<template>
  <q-page class="q-pa-md q-gutter-sm">

    <div style="width: 60%; height: 80%">
      <div class="row">

        <q-input
          v-model="file"
          :clearable="true"
          :model-value="file"
          accept=".xml"
          autofocus
          class="q-mx-sm"
          dense
          type="file"
          @clear="clrFile"
          @click="clickFile"
          @update:model-value="updFile"
          style="width: 500px"
        />


        <div>
          <q-btn
            :disable="!file || err || (file && isAnalyzed)"
            class="q-mx-sm"
            color="grey-4"
            icon="code"
            label="Анализ"
            text-color="black"
            :loading="loading"
            @click="fnAnalyze"
          >
            <template #loading>
              <q-spinner-hourglass color="white"/>
            </template>
          </q-btn>
        </div>

        <div>
          <q-btn
          :disable="!(file && err && isAnalyzed)"
            class="q-mx-sm"
            color="grey-4"
            icon="add_link"
            label="Привязка"
            text-color="black"
            @click="fnAssign"
          />
        </div>

        <div>
          <q-btn
            :disable="!isFilled"
            class="q-mx-sm"
            color="grey-4"
            icon="file_download"
            label="Залить"
            text-color="black"
            @click="fnFill"
          />
        </div>

        <div v-if="file && (isAnalyzed || isFilled)" class="text-black q-ma-sm">
          <div v-if="err">
            Анализ: <span class="text-red"> {{ msg }} </span>
          </div>
          <div v-else>
            Анализ: <span class="text-green"> Успешно </span>
          </div>
        </div>

      </div>

    </div>


    <div>
      <q-inner-loading :showing="loading" color="secondary"></q-inner-loading>
    </div>


    <div style="height: calc(100vh - 500px); width: 100%">

      <q-table
        :columns="cols"
        :rows="rows"
        :loading="loading"
        :rows-per-page-options="[0]"
        :wrap-cells="true"
        card-class="bg-amber-1 text-brown"
        color="primary"
        dense
        row-key="row"
        separator="cell"
        style="height: calc(100vh - 160px); width: 100%"
        table-header-class="text-bold text-white bg-blue-grey-13"
      >

      </q-table>

    </div>

  </q-page>
</template>

<script>
import {defineComponent, ref} from 'vue'
import {api} from "boot/axios.js";
import AssignPage from "pages/AssignPage.vue";

export default defineComponent({
  name: 'IndexPage',

  data() {
    return {
      file: ref(null),
      loading: false,
      err: false,
      msg: "",
      isFilled: false,
      isAnalyzed: false,
      isAssign: false,
      rows: [],
      cols: [],
      tableName: "Ball"
    }
  },

  methods: {


    clrFile() {
      this.file = ref(null)
      this.err = false
      this.msg = ""
      this.isFilled = false
      this.isAnalyzed = false
      this.rows = []
      this.cols = []
    },

    clickFile() {
      this.clrFile()
    },

    updFile(val) {
      if (val && val.length > 0) {
        this.file = val[0]

        if (this.file.name[0] === "B") {
          this.tableName = "Ball"
        } else if (this.file.name[0] === "G") {
          this.tableName = "Otstup"
        }
        this.cols = []
        this.cols = this.getColumns(this.tableName)
      }
    },

    fnAnalyze() {
      this.loading = true

      let fd = new FormData()
      fd.append('file', this.file)
      fd.append('filename', this.file.name)

      this.$axios
        .post('/importXml', fd, {
          headers: {
            'Content-Type': 'multipart/form-data'
          }
        })
        .then((response) => {
          //console.info("response.data", response.data)
          this.rows = response.data
        })
        .catch((error) => {
          console.log("error", error)
        })
        .finally(() => {
          api
            .post('', {
              method: "import/loadLog",
              params: [this.file.name],
            })
            .then(response => {
              //console.info("response", response.data.result.records[0])
              this.isAnalyzed = true
              this.msg = response.data.result.records[0].msg
              this.isFilled = response.data.result.records[0].filled === 1
              if (this.msg !== "")
                this.err = true
            })
            .finally(() => {
              this.isFilled = false
              this.rows.forEach((row) => {
                if (row["import"]===0) {
                  this.isFilled = true
                }
              })
            })
          //
        })
        .finally(()=> {
          this.loading = false
        })
    },

    fnAssign() {
      let cods = ""
      if (this.msg !== "")
        cods = this.msg.substring(this.msg.indexOf("[")+1, this.msg.length - 1)
      this.$q
        .dialog({
          component: AssignPage,
          componentProps: {
            cods: cods
          }
        })
        .onOk(() => {
          this.fnAnalyze()
        })
    },

    fnFill() {
      this.loading = true
      /*
            "CreatedAt": "2025-11-19",
            "UpdatedAt": "2025-11-19",
            "pvUser": 1087,
            "objUser": 1003,
       */
      let params = {
        "CreatedAt": "2025-11-19",
        "UpdatedAt": "2025-11-19",
        "pvUser": 1087,
        "objUser": 1003,
        "store": this.rows
      }

      api
        .post('', {
          method: "data/saveBallAndOtstupXml",
          params: [this.tableName, params],
        })
        .then(response => {
          console.info("response", response.data.result)
        })
        .catch((error)=> {
          console.error(error.message)
        })
        .finally(()=> {
          this.loading = false
        })

    },

    getColumns(tabl) {
      if (tabl === "Ball")
        return [
          {
            name: "rec",
            label: "rec",
            field: "rec",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:5%",
          },
          {
            name: "kod_napr",
            label: "kod_napr",
            field: "kod_napr",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "prizn_most",
            label: "prizn_most",
            field: "prizn_most",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "date_obn",
            label: "date_obn",
            field: "date_obn",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "nomer_mdk",
            label: "nomer_mdk",
            field: "nomer_mdk",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "avtor",
            label: "avtor",
            field: "avtor",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "km",
            label: "km",
            field: "km",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "pk",
            label: "pk",
            field: "pk",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "ballkm",
            label: "ballkm",
            field: "ballkm",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "kol_ots",
            label: "kol_ots",
            field: "kol_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "import",
            label: "import",
            field: "import",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
            format: (val) => (val ? 'Залито' : '')
          },


        ]
      else if (tabl === "Otstup")
        return [
          {
            name: "rec",
            label: "rec",
            field: "rec",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:5%",
          },
          {
            name: "kod_otstup",
            label: "kod_otstup",
            field: "kod_otstup",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "kod_napr",
            label: "kod_napr",
            field: "kod_napr",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "prizn_most",
            label: "prizn_most",
            field: "prizn_most",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "datetime_obn",
            label: "datetime_obn",
            field: "datetime_obn",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "nomer_mdk",
            label: "nomer_mdk",
            field: "nomer_mdk",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "avtor",
            label: "avtor",
            field: "avtor",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "km",
            label: "km",
            field: "km",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "pk",
            label: "pk",
            field: "pk",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "metr",
            label: "metr",
            field: "metr",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "dlina_ots",
            label: "dlina_ots",
            field: "dlina_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "velich_ots",
            label: "velich_ots",
            field: "velich_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "glub_ots",
            label: "glub_ots",
            field: "glub_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "stepen_ots",
            label: "stepen_ots",
            field: "stepen_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "kol_ots",
            label: "kol_ots",
            field: "kol_ots",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
          },
          {
            name: "import",
            label: "import",
            field: "import",
            align: "left",
            classes: "bg-blue-grey-1",
            headerStyle: "font-size: 1.2em; width:10%",
            format: (val) => (val ? 'Залито' : '')
          },


        ]
    }
  }

})
</script>
