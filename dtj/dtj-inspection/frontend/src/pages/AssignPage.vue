<template>

  <q-dialog
    ref="dialog"
    autofocus
    persistent
    style="min-width: 800px"
    transition-hide="slide-down"
    transition-show="slide-up"
    @hide="onDialogHide"
  >
    <q-card class="q-dialog-plugin" style="min-width: 800px; max-width: 800px">
      <q-bar class="text-white bg-primary">
        <div>Привязка</div>
      </q-bar>

      <q-card-section style="width: 800px">

        <q-table
          :columns="cols"
          :rows="rows"
          :rows-per-page-options="[0]"
          :wrap-cells="true"
          card-class="bg-amber-1 text-brown"
          color="primary"
          dense
          row-key="cod"
          separator="cell"
          table-header-class="text-bold text-white bg-blue-grey-13"
        >

          <template #body-cell="props">
            <q-td :props="props">

              <div v-if="props.col.field === 'cmd'">

                <q-btn
                  color="blue" dense flat icon="edit" round size="sm"
                  @click="fnEdit(props.row)"
                >
                  <q-tooltip
                    transition-hide="rotate" transition-show="rotate"
                  >
                    Редактирование
                  </q-tooltip>
                </q-btn>


              </div>

              <div v-else>
                {{ props.value }}
              </div>

            </q-td>
          </template>

        </q-table>


      </q-card-section>

      <q-card-actions align="right">
        <q-btn
          color="primary"
          dense
          icon="close"
          label="Закрыть"
          @click="onOkClick"
        />
      </q-card-actions>


    </q-card>

  </q-dialog>

</template>


<script>

/*import {api} from "boot/axios.js";*/

import {api} from "boot/axios.js";
import UpdAssign from "pages/UpdAssign.vue";

export default {
  props: ["tableName", "cods"],

  data() {
    return {
      rows: [],
      cols: []
    };
  },

  emits: [
    // REQUIRED
    "ok",
    "hide",
  ],

  methods: {
    load(cods) {
      this.loading = true
      api
        .post('', {
          method: 'import/loadAssign',
          params: [cods]
        })
        .then(
          (response) => {
            this.rows = response.data.result["records"]
            console.info("rows", this.rows)
          })
        .catch((error) => {
          console.log(error);
        })
        .finally(() => {
          this.loading = false
        })
    },

    fnEdit(row) {
      console.log(row);
      let prefix = row["cod"].substring(0, row["cod"].indexOf("_", 5))

      this.$q
        .dialog({
          component: UpdAssign,
          componentProps: {
            data: row,
            prefix: prefix,
          }
        })
        .onOk((r) => {
          console.info("r", r)
          console.info("row1", row)
          row.name = r.name
          console.info("row2", row)
        })


    },

    getColumns() {
      return [
        {
          name: "cod",
          label: "Код",
          field: "cod",
          align: "left",
          classes: "bg-blue-grey-1",
          headerStyle: "font-size: 1.2em; width:27%",
        },
        {
          name: "name",
          label: "Код ТОФИ",
          field: "name",
          align: "left",
          classes: "bg-blue-grey-1",
          headerStyle: "font-size: 1.2em; width:67%",
        },
        {
          name: "cmd",
          label: "",
          field: "cmd",
          align: "center",
          classes: "bg-blue-grey-1",
          headerStyle: "font-size: 1.2em; width:6%",
        },


      ]
    },

    // following method is REQUIRED
    // (don't change its name --> "show")
    show() {
      this.$refs.dialog.show();
    },

    // following method is REQUIRED
    // (don't change its name --> "hide")
    hide() {
      this.$refs.dialog.hide();
    },

    onDialogHide() {
      // required to be emitted
      // when QDialog emits "hide" event
      this.$emit("hide");
    },

    onOkClick() {
      // we just need to hide the dialog
      this.$emit("ok", {res: true})
      this.hide();
    },
  },

  created() {
    this.cols = this.getColumns()
    this.load(this.cods)
  },

}

</script>


<style scoped>

</style>
