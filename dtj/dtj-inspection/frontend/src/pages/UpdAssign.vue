<template>

  <q-dialog
    ref="dialog"
    autofocus
    persistent
    transition-hide="slide-down"
    transition-show="slide-up"
    @hide="onDialogHide"
  >
    <q-card class="q-dialog-plugin" style="width: 800px">
      <q-bar class="text-white bg-primary">
        <div>Редактировать</div>
      </q-bar>
      <q-card-section>

        <q-select
          v-model="form.id"
          :model-value="form.id"
          :options="optCods"
          class="q-mb-lg"
          dense
          label="Код ТОФИ"
          map-options
          option-label="name"
          option-value="id"
          use-input
          @filter="filterCod"
          @update:model-value="fnSelectCod"
        />

      </q-card-section>

      <q-card-actions align="right">
        <q-btn
          color="primary"
          dense
          icon="save"
          label="Сохранить"
          @click="onOkClick"
        />
        <q-btn
          color="primary"
          dense
          icon="close"
          label="Закрыть"
          @click="onCancelClick"
        />
      </q-card-actions>
    </q-card>

  </q-dialog>

</template>
<script>

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
        console.info("v", v)

        this.form.id = v.id
        this.form.syscodingcod = v.syscodingcod
        this.form.syscod = v.syscod
        this.form.syscoding = v.syscoding
        this.form.name = v.name
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
      this.loading = true
      let err = false
      api
        .post('', {
          method: "import/saveAssign",
          params: [this.form]
        })
        .then(
          () => {
            this.$emit("ok", {name: this.form.name})
          })
        .catch((error) => {
          err = true
          console.log(error);
        })
        .finally(() => {
          this.loading = false
          if (!err)
            this.hide()
        })
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
    //console.log("row", this.data)
    api
      .post('', {
        method: method,
        params: [this.data["cod"]]
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
