# -*- mode: ruby -*-
# vi: set ft=ruby :


Vagrant.configure(2) do |config|


  # Kubernetes Master Server
  config.vm.define "spark" do |spark|
    spark.vm.box = "centos/7"
    spark.vm.hostname = "spark.example.com"
    spark.vm.network "private_network", ip: "172.42.42.100"
    spark.vm.provider "virtualbox" do |v|
      v.name = "spark"
      v.memory = 8192
      v.cpus = 4
      # Prevent VirtualBox from interfering with host audio stack
      v.customize ["modifyvm", :id, "--audio", "none"]
    end
  end

  

end
