<script setup lang="ts">
import type { OperatorException } from '../types';

defineProps<{ exception?: OperatorException }>();
const emit = defineEmits<{ inspect: [] }>();
</script>

<template>
  <button v-if="exception" class="reconcile-banner" type="button" @click="emit('inspect')">
    <span class="reconcile-icon"><UIcon name="i-mdi-information-outline" /></span>
    <strong>{{ exception.title }}</strong>
    <span class="reconcile-detail">·&nbsp; {{ exception.detail.replace(' · 新订单已阻止', '') }}</span>
    <span class="reconcile-action">查看 <UIcon name="i-mdi-chevron-right" /></span>
  </button>
</template>
