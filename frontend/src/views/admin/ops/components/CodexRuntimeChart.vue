<script setup lang="ts">
import { computed } from 'vue'
import { Chart as ChartJS, CategoryScale, Legend, LineElement, LinearScale, PointElement, Title, Tooltip } from 'chart.js'
import { Line } from 'vue-chartjs'
import type { OpsCodexRuntimePoint } from '@/api/admin/ops'
import EmptyState from '@/components/common/EmptyState.vue'

ChartJS.register(Title, Tooltip, Legend, LineElement, LinearScale, PointElement, CategoryScale)

const props = defineProps<{
  points: OpsCodexRuntimePoint[]
  loading: boolean
}>()

function hm(iso: string): string {
  const d = new Date(iso)
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

const chartData = computed(() => {
  if (!props.points.length) return null
  return {
    labels: props.points.map((p) => hm(p.created_at)),
    datasets: [
      {
        label: '内存 MB',
        data: props.points.map((p) => p.memory_used_mb ?? null),
        borderColor: '#2dd4bf',
        yAxisID: 'y',
        tension: 0.25,
        pointRadius: 0
      },
      {
        label: 'Heap Alloc MB',
        data: props.points.map((p) => p.heap_alloc_mb ?? null),
        borderColor: '#4aa3ff',
        yAxisID: 'y',
        tension: 0.25,
        pointRadius: 0
      },
      {
        label: 'Goroutines',
        data: props.points.map((p) => p.goroutine_count ?? null),
        borderColor: '#a371f7',
        yAxisID: 'y1',
        tension: 0.25,
        pointRadius: 0
      },
      {
        label: 'WS 活跃连接',
        data: props.points.map((p) => p.ws_active_conns ?? null),
        borderColor: '#e3b341',
        yAxisID: 'y1',
        tension: 0.25,
        pointRadius: 0
      }
    ]
  }
})

const options = {
  responsive: true,
  maintainAspectRatio: false,
  animation: false as const,
  interaction: { mode: 'index' as const, intersect: false },
  plugins: {
    legend: {
      position: 'top' as const,
      align: 'end' as const,
      labels: { usePointStyle: true, boxWidth: 6, font: { size: 10 } }
    }
  },
  scales: {
    y: { type: 'linear' as const, position: 'left' as const, title: { display: true, text: 'MB' } },
    y1: {
      type: 'linear' as const,
      position: 'right' as const,
      grid: { drawOnChartArea: false },
      title: { display: true, text: '数量' }
    }
  }
}

const state = computed(() => {
  if (chartData.value) return 'ready'
  if (props.loading) return 'loading'
  return 'empty'
})
</script>

<template>
  <div class="flex h-full flex-col rounded-3xl bg-white p-6 shadow-sm ring-1 ring-gray-900/5 dark:bg-dark-800 dark:ring-dark-700">
    <h3 class="mb-4 text-sm font-bold text-gray-900 dark:text-white">Codex 监控 · 运行时指标</h3>
    <div class="min-h-[260px] flex-1">
      <Line v-if="state === 'ready' && chartData" :data="chartData" :options="options" />
      <div v-else class="flex h-full min-h-[260px] items-center justify-center">
        <div v-if="state === 'loading'" class="animate-pulse text-sm text-gray-400">加载中…</div>
        <EmptyState v-else />
      </div>
    </div>
  </div>
</template>
