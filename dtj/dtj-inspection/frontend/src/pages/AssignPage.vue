<template>

  <q-dialog
    ref="dialog"
    @hide="onDialogHide"
    persistent
    autofocus
    transition-show="slide-up"
    transition-hide="slide-down"
  >
    <q-card class="q-dialog-plugin" style="width: 800px">
      <q-bar class="text-white bg-primary">
        <div>Привязка</div>
      </q-bar>

      <q-card-section>

        <q-table
          :columns="cols"
          :rows="rows"
          :wrap-cells="true"
          card-class="bg-amber-1 text-brown"
          color="primary"
          dense
          row-key="cod"
          separator="cell"
          table-header-class="text-bold text-white bg-blue-grey-13"
          :rows-per-page-options="[0]"
         >

          <template #body-cell="props">
            <q-td :props="props">

              <div v-if="props.col.field === 'cmd'">

                <q-btn
                       round size="sm" icon="edit" color="blue" flat dense
                       @click="fnEdit(props.row)"
                >
                  <q-tooltip
                    transition-show="rotate" transition-hide="rotate"
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
          dense
          color="primary"
          icon="close"
          label="Закрыть"
          @click="onCancelClick"
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
        .onOk(() => {

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
          name: "cod_tofi",
          label: "Код ТОФИ",
          field: "cod_tofi",
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

      onCancelClick() {
      // we just need to hide the dialog
      this.hide();
    },
  },

  created() {
    this.cols = this.getColumns()
    this.loading = true
    api
      .post('', {
        method: 'import/loadAssign',
        params: [/*this.tableName, */this.cods]
      })
      .then(
        (response) => {
          this.rows = response.data.result["records"]
        })
      .catch((error) => {
        console.log(error);
      })
      .finally(() => {
        this.loading = false
      })

  },

}

</script>


<style scoped>

</style>
