// src/frontend/app.js
const { createApp, ref, onMounted, computed } = Vue;
createApp({
    setup() {
        const token = '__DISTGET_TOKEN__';
        const peers = ref([]);
        const tasks = ref([]);
        const activeTab = ref('downloading');
        
        const getLocal = (k, d) => { try { return localStorage.getItem(k) || d; } catch { return d; } };
        const setLocal = (k, v) => { try { localStorage.setItem(k, v); } catch {} };

        const currentLang = ref(getLocal('lang', 'en'));
        const currentTheme = ref(getLocal('theme', 'dark'));
        const isDark = computed(() => currentTheme.value === 'dark');

        const showNewTaskModal = ref(false);
        const newTaskUrls = ref('');
        const parsedTasks = ref([]);
        const newTaskDir = ref('');
        const isSubmitting = ref(false);

        const showSettingsModal = ref(false);
        const sysConfig = ref(null);
        const sysInfo = ref(null);

        const showShareModal = ref(false);
        const isAddingPeer = ref(false);
        const shareString = ref('');
        const importStr = ref('');
        const newPeer = ref({ address: '', token: '' });

        const selectedTasks = ref([]);
        const expandedTasks = ref([]);
        const calculatingChecksums = ref(new Set());

        const showConflictModal = ref(false);
        const conflictTasks = ref([]);

        const toasts = ref([]);
        let toastId = 0;
        const showToast = (message, type = 'info') => {
            const id = toastId++;
            toasts.value.push({ id, message, type });
            setTimeout(() => {
                toasts.value = toasts.value.filter(t => t.id !== id);
            }, 3000);
        };

        const t = (key) => {
            const extra = {
                en: { 
                    useProxy: 'Use System Proxy', 
                    confirmDelete: 'Are you sure to delete?', 
                    confirmDeleteAll: 'Are you sure to clear all completed tasks?', 
                    logLevel: 'Log Level',
                    shareNode: 'Share Known Peers',
                    shareable: 'Shareable',
                    localNode: 'Local Node',
                    maxTasks: 'Max Tasks',
                    taskExistsConfirm: 'The following tasks already exist. Do you want to re-download? Re-downloading will overwrite the original files.',
                    taskExistsTitle: 'Task Already Exists',
                    reDownload: 'Re-download'
                },
                zh: { 
                    useProxy: '使用系统代理', 
                    confirmDelete: '确定要删除吗？', 
                    confirmDeleteAll: '确定要清空所有已完成的任务吗？', 
                    logLevel: '日志等级',
                    shareNode: '分享已知节点',
                    shareable: '允许被分享',
                    localNode: '本机节点',
                    maxTasks: '最大任务数',
                    taskExistsConfirm: '以下任务已存在，是否重新下载？重新下载将覆盖原文件。',
                    taskExistsTitle: '任务已存在',
                    reDownload: '重新下载'
                }
            };
            if (extra[currentLang.value] && extra[currentLang.value][key]) return extra[currentLang.value][key];
            if (typeof locales !== 'undefined' && locales[currentLang.value] && locales[currentLang.value][key]) return locales[currentLang.value][key];
            return key;
        };

        const toggleLang = () => { currentLang.value = currentLang.value === 'en' ? 'zh' : 'en'; setLocal('lang', currentLang.value); };
        const toggleTheme = () => { 
            currentTheme.value = currentTheme.value === 'dark' ? 'light' : 'dark'; 
            setLocal('theme', currentTheme.value);
            document.documentElement.classList.toggle('dark', currentTheme.value === 'dark');
        };

        const formatBytes = (bytes) => {
            if (bytes === undefined || bytes === null || bytes === 0) return '0 B';
            const k = 1024, sizes = ['B', 'KB', 'MB', 'GB', 'TB'], i = Math.floor(Math.log(bytes) / Math.log(k));
            const value = bytes / Math.pow(k, i);
            
            if (value >= 1000) {
                return parseFloat(value.toPrecision(4)) + ' ' + sizes[i];
            } else if (value >= 100) {
                return parseFloat(value.toFixed(1)) + ' ' + sizes[i];
            } else if (value >= 10) {
                return parseFloat(value.toFixed(2)) + ' ' + sizes[i];
            } else {
                return parseFloat(value.toPrecision(2)) + ' ' + sizes[i];
            }
        };
        const formatSpeed = (bps) => bps ? formatBytes(bps) + '/s' : '0 B/s';
        const getFilename = (url) => { try { return url.split('/').pop().split('?')[0] || url; } catch { return url; } };
        const formatTime = (timestamp) => {
            if (!timestamp) return '--:--';
            const date = new Date(timestamp * 1000);
            const hours = date.getHours().toString().padStart(2, '0');
            const minutes = date.getMinutes().toString().padStart(2, '0');
            return `${hours}:${minutes}`;
        };

        const calculateETA = (task) => {
            if (task.status === 'Resolving' || task.status === 'Pending') return '...';
            if (task.download_speed <= 0 || task.file_size === 0) return '--:--:--';
            const seconds = Math.floor((task.file_size - task.downloaded) / task.download_speed);
            if (!isFinite(seconds)) return '--:--:--';
            return `${Math.floor(seconds / 3600).toString().padStart(2, '0')}:${Math.floor((seconds % 3600) / 60).toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}`;
        };

        const sortedPeers = computed(() => {
            return[...peers.value].sort((a, b) => {
                if (a.is_self) return -1;
                if (b.is_self) return 1;
                
                if (a.status === 'Online' && b.status !== 'Online') return -1;
                if (a.status !== 'Online' && b.status === 'Online') return 1;
                
                if (a.status === 'Online' && b.status === 'Online') {
                    const speedA = a.download_speed || 0;
                    const speedB = b.download_speed || 0;
                    if (Math.abs(speedA - speedB) > 0.1) {
                        return speedB - speedA;
                    }
                    return (a.latency_ms || 999) - (b.latency_ms || 999);
                }
                
                return a.node_id.localeCompare(b.node_id);
            });
        });
        
        const activeTasks = computed(() => tasks.value.filter(t => t.status !== 'Completed'));
        const completedTasks = computed(() => tasks.value.filter(t => t.status === 'Completed'));

        const getStatusClass = (status) => {
            if (status === 'Running') return 'bg-accent/20 text-accent';
            if (status === 'Resolving') return 'bg-purple-500/20 text-purple-500';
            if (status === 'Pending') return 'bg-blue-400/20 text-blue-400';
            if (status === 'Paused') return 'bg-warning/20 text-warning';
            if (status === 'Completed') return 'bg-success/20 text-success';
            if (status.includes('Failed')) return 'bg-danger/20 text-danger';
            return 'bg-gray-500/20 text-gray-500';
        };

        const fetchOptions = (signal) => ({ headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }, signal });
        const fetchWithTimeout = async (url, options = {}, timeout = 10000) => {
            const controller = new AbortController();
            const id = setTimeout(() => controller.abort(), timeout);
            try {
                const res = await fetch(url, { ...options, signal: controller.signal });
                clearTimeout(id);
                return res;
            } catch (e) {
                clearTimeout(id);
                if (e.name === 'AbortError') throw new Error('Request timeout');
                throw e;
            }
        };

        const fetchSysInfo = async () => {
            try {
                const res = await fetchWithTimeout('/api/v1/sys/info', fetchOptions(), 5000);
                if (res.ok) sysInfo.value = (await res.json()).data;
            } catch (e) { console.error('fetchSysInfo error:', e); }
        };

        const fetchData = async () => {
            try {
                const [pRes, tRes] = await Promise.all([
                    fetchWithTimeout('/api/v1/peers', fetchOptions(), 5000),
                    fetchWithTimeout('/api/v1/tasks', fetchOptions(), 5000)
                ]);
                if (pRes.ok) peers.value = (await pRes.json()).data ||[];
                if (tRes.ok) tasks.value = (await tRes.json()).data ||[];
            } catch (e) { console.error('fetchData error:', e); }
        };

        const openNewTaskModal = async () => {
            showNewTaskModal.value = true;
            newTaskUrls.value = '';
            parsedTasks.value =[];
            try {
                const res = await fetchWithTimeout('/api/v1/sys/config', fetchOptions(), 5000);
                if (res.ok) {
                    const data = await res.json();
                    newTaskDir.value = data.data?.default_save_dir || '';
                }
            } catch (e) { console.error('openNewTaskModal error:', e); }
        };

        const parseUrls = () => {
            const urls = newTaskUrls.value.split('\n').map(u => u.trim()).filter(u => u);
            parsedTasks.value = urls.map(url => ({ url, file_name: getFilename(url) }));
        };

        const submitNewTask = async () => {
            if (parsedTasks.value.length === 0) {
                showToast('Please enter at least one URL', 'error');
                return;
            }

            const conflicts =[];
            for (const pt of parsedTasks.value) {
                const existing = tasks.value.find(t => t.url === pt.url);
                if (existing) conflicts.push(existing);
            }

            if (conflicts.length > 0) {
                conflictTasks.value = conflicts;
                showConflictModal.value = true;
                return;
            }

            await executeBatchSubmit(parsedTasks.value);
        };

        const confirmReDownload = async () => {
            parsedTasks.value.forEach(pt => pt.force = true);
            showConflictModal.value = false;
            await executeBatchSubmit(parsedTasks.value);
        };

        const cancelReDownload = () => {
            showConflictModal.value = false;
        };

        const executeBatchSubmit = async (tasksToSubmit) => {
            isSubmitting.value = true;
            try {
                const res = await fetchWithTimeout('/api/v1/tasks/batch', { method: 'POST', ...fetchOptions(), body: JSON.stringify({ tasks: tasksToSubmit, save_dir: newTaskDir.value }) }, 30000);
                if (!res.ok) {
                    const json = await res.json();
                    showToast('Failed: ' + (json.message || 'Unknown error'), 'error');
                    return;
                }
                showNewTaskModal.value = false;
                activeTab.value = 'downloading';
                fetchData();
            } catch (e) {
                console.error('Submit error:', e);
                showToast('Network error: ' + e.message, 'error');
            } finally {
                isSubmitting.value = false;
            }
        };

        const controlTask = async (id, action) => {
            if (action === 'delete' && !confirm(t('confirmDelete'))) return;
            try {
                const res = await fetchWithTimeout(`/api/v1/tasks/${id}${action === 'delete' ? '' : '/' + action}`, { 
                    method: action === 'delete' ? 'DELETE' : 'POST', 
                    ...fetchOptions(),
                    body: action === 'delete' ? undefined : JSON.stringify({})
                }, 15000);
                if (!res.ok) {
                    const json = await res.json();
                    showToast(json.message || 'Action failed', 'error');
                }
                fetchData();
            } catch (e) {
                console.error('controlTask error:', e);
                showToast('Network error: ' + e.message, 'error');
            }
        };

        const toggleSelectAll = () => selectedTasks.value = selectedTasks.value.length === activeTasks.value.length && activeTasks.value.length > 0 ?[] : activeTasks.value.map(t => t.task_id);

        const getBatchPauseResumeAction = () => {
            if (selectedTasks.value.length === 0) return 'pause';
            
            const selectedTaskObjects = tasks.value.filter(t => selectedTasks.value.includes(t.task_id));
            const allNotRunning = selectedTaskObjects.every(task => 
                task.status === 'Paused' || 
                task.status.includes('Failed') || 
                task.status === 'Completed' || 
                task.status === 'Pending'
            );
            
            return allNotRunning ? 'resume' : 'pause';
        };

        const batchAction = async (action) => {
            if (action === 'deleteAllCompleted') {
                if (!confirm(t('confirmDeleteAll'))) return;
                for (const t of completedTasks.value) await controlTask(t.task_id, 'delete');
                return;
            }
            if (selectedTasks.value.length === 0 || (action === 'delete' && !confirm(t('confirmDelete')))) return;
            for (const id of selectedTasks.value) {
                const task = tasks.value.find(t => t.task_id === id);
                if (!task) continue;
                if (action === 'pauseResume') {
                    const batchActionType = getBatchPauseResumeAction();
                    await controlTask(id, batchActionType);
                } else if (action === 'delete') await controlTask(id, 'delete');
            }
            if (action === 'delete') selectedTasks.value =[];
        };

        const copyUrl = (url) => { navigator.clipboard.writeText(url); showToast(t('copied'), 'success'); };
        const openDir = async (task) => {
            try {
                const res = await fetchWithTimeout(`/api/v1/tasks/${task.task_id}/open`, { method: 'POST', ...fetchOptions(), body: JSON.stringify({}) }, 5000);
                if (!res.ok) throw new Error();
            } catch (e) {
                copyUrl(task.save_path);
                showToast(t('openFailed'), 'error');
            }
        };
        
        const recalcChecksums = async (id) => {
            calculatingChecksums.value.add(id);
            try {
                const res = await fetchWithTimeout(`/api/v1/tasks/${id}/checksums`, { method: 'POST', ...fetchOptions(), body: JSON.stringify({}) }, 30000);
                if (res.ok) {
                    fetchData();
                } else {
                    const json = await res.json();
                    showToast(json.message || 'Failed to calculate checksums', 'error');
                }
            } catch (e) {
                showToast('Network error: ' + e.message, 'error');
            } finally { 
                calculatingChecksums.value.delete(id); 
            }
        };

        const toggleExpand = (id) => { const i = expandedTasks.value.indexOf(id); i === -1 ? expandedTasks.value.push(id) : expandedTasks.value.splice(i, 1); };

        const openSettingsModal = async () => {
            showSettingsModal.value = true;
            sysConfig.value = null;
            try {
                const res = await fetchWithTimeout('/api/v1/sys/config', fetchOptions(), 5000);
                if (res.ok) { 
                    const data = (await res.json()).data; 
                    if (data) {
                        for (const key in data) {
                            if (data[key] === null) data[key] = '';
                        }
                        if (!data.log_level) data.log_level = 'info';
                        if (data.shareable === undefined || data.shareable === '') data.shareable = true;
                        sysConfig.value = data; 
                    } else {
                        showToast('Failed to load settings: no data', 'error');
                        showSettingsModal.value = false;
                    }
                } else { 
                    console.error('Failed to fetch config:', res.status); 
                    showToast('Failed to load settings: ' + res.status, 'error');
                    showSettingsModal.value = false;
                }
            } catch (e) { 
                console.error('Error opening settings:', e); 
                showToast('Network error: ' + e.message, 'error'); 
                showSettingsModal.value = false;
            }
        };
        const saveSettings = async () => {
            try {
                const payload = { ...sysConfig.value };
                for (const key in payload) {
                    if (payload[key] === '') {
                        payload[key] = null;
                    } else if (['grpc_port', 'api_port', 'max_connections', 'max_tasks', 'global_speed_limit_kb', 'peer_speed_limit_kb'].includes(key)) {
                        payload[key] = Number(payload[key]);
                    }
                }
                const res = await fetchWithTimeout('/api/v1/sys/config', { method: 'PUT', ...fetchOptions(), body: JSON.stringify(payload) }, 5000);
                if (!res.ok) {
                    const json = await res.json();
                    showToast('Failed to save: ' + (json.message || res.status), 'error');
                } else {
                    showSettingsModal.value = false;
                }
            } catch (e) { showToast('Failed to save: ' + e.message, 'error'); }
        };

        const openShareModal = async () => {
            isAddingPeer.value = false; 
            showShareModal.value = true;
            try {
                const res = await fetchWithTimeout('/api/v1/sys/config', fetchOptions(), 5000);
                if (res.ok) {
                    const conf = (await res.json()).data;
                    const ip = conf.public_address || `127.0.0.1:${conf.grpc_port}`;
                    const obj = { address: ip, token: token };
                    shareString.value = `octoget://${btoa(JSON.stringify(obj))}`;
                } else {
                    showToast('Failed to load config', 'error');
                    showShareModal.value = false;
                }
            } catch (e) {
                showToast('Network error: ' + e.message, 'error');
                showShareModal.value = false;
            }
        };
        const openAddPeerModal = () => { isAddingPeer.value = true; importStr.value = ''; newPeer.value = { address: '', token: '' }; showShareModal.value = true; };
        const parseImportStr = () => {
            if (importStr.value.startsWith('octoget://')) {
                try {
                    const obj = JSON.parse(atob(importStr.value.replace('octoget://', '')));
                    newPeer.value = { address: obj.address || '', token: obj.token || '' };
                } catch (e) {}
            }
        };
        const submitNewPeer = async () => {
            if (!newPeer.value.address || !newPeer.value.token) return;
            try {
                const res = await fetchWithTimeout('/api/v1/peers', { method: 'POST', ...fetchOptions(), body: JSON.stringify(newPeer.value) }, 10000);
                if (res.ok) { showShareModal.value = false; fetchData(); }
                else { const json = await res.json(); showToast('Failed to add peer: ' + (json.message || 'Unknown error'), 'error'); }
            } catch (e) { console.error('Error adding peer:', e); showToast('Failed to add peer: ' + e.message, 'error'); }
        };

        onMounted(() => { 
            try {
                document.documentElement.classList.toggle('dark', isDark.value); 
                fetchSysInfo();
                fetchData(); 
                setInterval(fetchData, 2000); 
            } catch (e) {
                console.error("Initialization error:", e);
            }
        });

        return {
            peers, tasks, activeTasks, completedTasks, sortedPeers, formatBytes, formatSpeed, formatTime, getFilename, calculateETA, getStatusClass,
            isDark, activeTab, t, toggleLang, toggleTheme, showNewTaskModal, newTaskUrls, parsedTasks, parseUrls, newTaskDir, isSubmitting, submitNewTask, openNewTaskModal,
            controlTask, batchAction, selectedTasks, toggleSelectAll, copyUrl, openDir, recalcChecksums, expandedTasks, toggleExpand, calculatingChecksums,
            showSettingsModal, sysConfig, sysInfo, openSettingsModal, saveSettings, showShareModal, isAddingPeer, shareString, importStr, newPeer, openShareModal, openAddPeerModal, parseImportStr, submitNewPeer,
            getBatchPauseResumeAction,
            showConflictModal, conflictTasks, confirmReDownload, cancelReDownload, toasts
        };
    }
}).mount('#app');