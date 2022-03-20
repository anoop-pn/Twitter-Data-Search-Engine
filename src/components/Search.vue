<template>
  <v-container class="back" fluid align="center">
    <v-row align="center" justify="center">
      <div class="text-center">
        <img src="../assets/twitter.png">
        <v-text-field
        label="Enter your search query here"
        v-model="query"
        @keyup.enter="executeQuery"
        size="50"
        ></v-text-field>
        <v-radio-group v-model="queryType">
          <v-radio
           label="Lucene Index"
           :value="lucene"/>
          <v-radio
           label="Hadoop Index"
           :value="hadoop"/>
        </v-radio-group>
        <v-btn 
        color="primary"
        elevation="2"
        @click="executeQuery">
        Search
        </v-btn>
        <br><br>
        <v-progress-circular
        :size="50"
        color="amber"
        indeterminate
        v-if="this.loading"
        ></v-progress-circular>
          <v-dialog
          max-width="500px"
          v-model="alert"
          >
            <v-card>
            <v-card-title>{{alertMessage}}</v-card-title>
            </v-card>
            </v-dialog>
            <v-dialog
            v-model="dialog"
            fullscreen
            hide-overlay
            transition="dialog-bottom-transition"
            >
              <v-card
                elevation="2"
              >
              <v-toolbar
                dark
                color="primary"
              >
              <v-btn
                icon
                dark
                @click="dialog = false"
              >
              <v-icon>mdi-close</v-icon>
              </v-btn>
              <v-toolbar-title>Search Results</v-toolbar-title>
              <v-spacer></v-spacer>
              </v-toolbar>
              <v-simple-table  align="center">
                <template v-slot:default>
                <thead>
                  <tr>
                    <th> Rank</th>
                    <th> Rank Score</th>
                    <th> Tweet</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="data in result" :key="data.rank" >
                    <td> {{ data.rank }} </td>
                    <td> {{ data.score }} </td>
                    <td> {{ data.text }} </td>
                  </tr>
                </tbody>
                </template>
              </v-simple-table>
              </v-card>
          </v-dialog>
      </div>
    </v-row>
  </v-container>
</template>

<script>
export default {
  name: 'search-app',
  data () {
    return {
      // Declaring variables here
      query: '',
      queryType: 'luceneIndex',
      lucene: 'luceneIndex',
      hadoop: 'hadoopIndex',
      result: null,
      tableView: false,
      loading: false,
      dialog: false,
      alert: false,
      alertMessage: ''
    }
  },
  methods: {
    async executeQuery () {
      this.tableView = false
      this.result = null
      if (this.query.length === 0) {
        this.alert = true
        this.alertMessage = "Please input a search query."
      } else {
          try {
            this.loading = true
            const response = await this.$axios.get('http://localhost:8083/'+this.queryType+'?searchQuery=' + this.query)
            this.result = response.data
            console.log(this.result)
            if (Object.keys(this.result).length === 0) {
              this.loading = false
              this.alert = true
              this.alertMessage = "Sorry! No results found.."
            } else {
              this.tableView = true
              this.dialog = true
            }
          } catch (error) {
            this.alert = true
            this.alertMessage = "Sorry! Something went wrong in the backend.."
            console.log(error)
          } finally {
            this.loading = false;
            this.query = ''
          }
       }
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.center {
  text-align: center;
}
.tr, td {
  text-align: left;
}
table, td {
  border: 1px solid;
}
th {
  font-size: 1.5rem !important;
  font-weight: bold;
  border: 1px solid;
}
</style>