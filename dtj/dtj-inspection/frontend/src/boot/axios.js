import { defineBoot } from '#q-app/wrappers'
import axios from 'axios'

let url = 'http://127.0.0.1:8080'
let baseURL = url + "/api"

if (import.meta.env.PROD) {
  baseURL = "/api"
}

const api = axios.create({ baseURL: baseURL })

export default defineBoot(({ app }) => {
  // for use inside Vue files (Options API) through this.$axios and this.$api

  app.config.globalProperties.$axios = axios
  // ^ ^ ^ this will allow you to use this.$axios (for Vue Options API form)
  //       so you won't necessarily have to import axios in each vue file

  app.config.globalProperties.$api = api
  // ^ ^ ^ this will allow you to use this.$api (for Vue Options API form)
  //       so you can easily perform requests against your app's API
})


export { api, baseURL };
