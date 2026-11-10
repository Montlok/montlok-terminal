import { createPinia } from 'pinia';
import ui from '@nuxt/ui/vue-plugin';

import App from './App.vue';
import './styles/tailwind.css';

const myApp = createApp(App);

const pinia = createPinia();
myApp.use(pinia);

myApp.use(ui);
myApp.mount('#app');
