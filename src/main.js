import Vue from 'vue'
import App from './App'
import axios from 'axios'
import Vuetify from 'vuetify'
import 'vuetify/dist/vuetify.min.css'
import "@mdi/font/css/materialdesignicons.css";

Vue.prototype.$axios = axios
Vue.use(Vuetify)


new Vue({
  vuetify: new Vuetify(),
  components: { App },
  render: h => h(App)
}).$mount('#app')