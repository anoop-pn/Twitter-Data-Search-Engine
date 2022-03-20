<template>
  <v-app>
    <v-main>
      <v-app-bar
        color="light-blue accent-4"
        dense
        dark
      >
        <v-spacer />
          <v-toolbar-title>{{msg}}</v-toolbar-title>
        <v-spacer />
      </v-app-bar>
      <!-- Adding Search.vue page here -->
      <Search />
    </v-main>
  </v-app>
</template>

<script>
import Search from './components/Search.vue'

export default {
  name: 'app',
  data() {
    return {
      msg: 'Twitter Data Search Engine'
    }
  },
  components: {
    Search
  }
}
</script>
<style>
</style>