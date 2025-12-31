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
        <div>Редактировать</div>
      </q-bar>
      <q-card-section>

        <q-select
          v-model="form.id"
          :model-value="form.id"
          label="Код ТОФИ"
          :options="optCods"
          dense
          map-options
          option-label="name"
          option-value="id"
          class="q-mb-lg"
          use-input
          @update:model-value="fnSelectCod"
          @filter="filterCod"
        />



      </q-card-section>

      <q-card-actions align="right">
        <q-btn
          dense
          color="primary"
          icon="save"
          label="Сохранить"
          @click="onOkClick"
        />
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

export default {
  props: ["data", "prefix"],

  data() {
    return {
      form: this.data,
      optCods: [],
      optCodsOrg: [],
    };
  },

  emits: [
    // REQUIRED
    "ok",
    "hide",
  ],

  methods: {

    fnSelectCod(v) {
      if (v) {
        this.form.id = v.id
        this.form.tofi_cod = v.tofi_cod
      }
    },

    filterCod(val, update) {
      if (val === null || val === '') {
        update(() => {
          this.optCods = this.optCodsOrg
        })
        return
      }
      update(() => {
        if (this.optCodsOrg.length < 2) return
        const needle = val.toLowerCase()
        let name = 'name'
        this.optCods = this.optCodsOrg.filter((v) => {
          return v[name].toLowerCase().indexOf(needle) > -1
        })
      })
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

    },


      onCancelClick() {
      // we just need to hide the dialog
      this.hide();
    },
  },

  created() {
    this.loading = true
    let method = "import/loadObjForSelect"
    if (this.prefix === "kod_otstup")
      method = "import/loadRelObjForSelect"
    api
      .post('', {
        method: method,
        params: []
      })
      .then(
        (response) => {
          this.optCods = response.data.result["records"]
          this.optCodsOrg = response.data.result["records"]
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
