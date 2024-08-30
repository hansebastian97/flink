Vagrant.configure("2") do |config|
  config.hostmanager.enabled = true 
  config.hostmanager.manage_host = true
  config.vbguest.auto_update = false
  config.ssh.insert_key = false
  
  ### Ansible ###
config.vm.define "flink" do |flink|
  flink.vm.box = "geerlingguy/ubuntu2004"
  flink.vm.hostname = "flink"
  flink.vm.network "private_network", ip: "192.168.60.60", virtualbox__intnet: false
  flink.vm.synced_folder ".\\vagrant_folder", "/vagrant"
  flink.vm.provider "virtualbox" do |vb|
    # vb.gui = true
    vb.memory = "6000"
    vb.name = "flink"
    vb.cpus = "4"
    vb.customize ["modifyvm", :id, "--uart1", "0x3F8", "4"]
    vb.customize ["modifyvm", :id, "--uartmode1", "file", File::NULL]
  end
  flink.vm.provision "shell", path: ".\\vagrant_folder\\docker.sh"
end
end  
